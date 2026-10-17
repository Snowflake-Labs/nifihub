# Copyright 2026 Snowflake Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/usr/bin/env python3
"""Deploy a flow from JSON to an existing NiFi test instance with full CD-style setup.

Uploads the flow JSON, adds inherited parameter contexts from providers,
applies parameters and assets, then starts the flow.

Usage:
    python scripts/ci/deploy_ci_flow.py \
        --flow-path flows/data-generator/postgres-cdc-demo.json \
        --config flows/data-generator/tests/test_postgres_cdc_demo.yaml \
        --runtime-url https://localhost:8443 \
        --token <token> \
        --output-file /tmp/pg_id.txt
"""
import argparse
import json
import os
import re
import sys
import tempfile
import uuid

import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "cd"))

import nipyapi
import yaml
from jsonschema import validate, FormatChecker
from nifi_config import get_nifi_auth

from manage_flows import configure_nifi, find_flow_pg_by_name, start_flow, get_root_pg_id
from manage_parameters import reconcile_flow_parameters, add_inherited_parameter_contexts, resolve_value, apply_parameter_overrides
from manage_assets import reconcile_flow_assets
from manage_parameter_providers import reconcile_parameter_providers
from manage_controller_services import reconcile_controller_services
from setup_registry_client import setup as setup_registry
import manage_parameter_providers  # noqa: F401 — triggers monkey patch


def resolve_nar_url(url):
    prefix = "${GITHUB_RELEASES}/"
    if url.startswith(prefix):
        server = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
        repo = os.environ.get("GITHUB_REPOSITORY", "")
        if not repo:
            raise ValueError("GITHUB_REPOSITORY env var not set — cannot resolve ${GITHUB_RELEASES}")
        base = f"{server}/{repo}/releases/download"
        return base + "/" + url[len(prefix):]
    return url


def download_nar(url):
    headers = {}
    token = os.environ.get("GITHUB_TOKEN", "")
    if "github.com" in url and "/releases/download/" in url:
        parts = url.replace("https://github.com/", "").split("/releases/download/")
        if len(parts) == 2 and token:
            repo = parts[0]
            tag_and_asset = parts[1]
            tag = tag_and_asset.rsplit("/", 1)[0]
            asset_name = tag_and_asset.rsplit("/", 1)[1]
            api_url = f"https://api.github.com/repos/{repo}/releases/tags/{tag}"
            api_headers = {
                "Authorization": f"token {token}",
                "Accept": "application/vnd.github+json",
            }
            print(f"[ci-deploy] Fetching release info for {repo} tag {tag}...", file=sys.stderr)
            rel_resp = requests.get(api_url, headers=api_headers, timeout=30)
            rel_resp.raise_for_status()
            assets = rel_resp.json().get("assets", [])
            asset_id = None
            for a in assets:
                if a["name"] == asset_name:
                    asset_id = a["id"]
                    break
            if asset_id is None:
                raise ValueError(f"Asset '{asset_name}' not found in release {tag}")
            download_url = f"https://api.github.com/repos/{repo}/releases/assets/{asset_id}"
            headers = {
                "Authorization": f"token {token}",
                "Accept": "application/octet-stream",
            }
            print(f"[ci-deploy] Downloading NAR via API: {asset_name}", file=sys.stderr)
            resp = requests.get(download_url, headers=headers, allow_redirects=False, timeout=30)
            if resp.status_code in (301, 302, 307, 308):
                redirect_url = resp.headers["Location"]
                resp = requests.get(redirect_url, timeout=120)
            resp.raise_for_status()
            tmp = tempfile.NamedTemporaryFile(suffix=".nar", delete=False)
            tmp.write(resp.content)
            tmp.close()
            print(f"[ci-deploy] Downloaded {asset_name} ({len(resp.content)} bytes)", file=sys.stderr)
            return tmp.name, asset_name
    if "github.com" in url and token:
        headers["Authorization"] = f"token {token}"
    print(f"[ci-deploy] Downloading NAR: {url}", file=sys.stderr)
    resp = requests.get(url, headers=headers, allow_redirects=True, timeout=120)
    resp.raise_for_status()
    filename = url.split("/")[-1]
    tmp = tempfile.NamedTemporaryFile(suffix=".nar", delete=False)
    tmp.write(resp.content)
    tmp.close()
    print(f"[ci-deploy] Downloaded {filename} ({len(resp.content)} bytes)", file=sys.stderr)
    return tmp.name, filename


def upload_nars(nar_urls):
    if not nar_urls:
        return
    for url in nar_urls:
        resolved = resolve_nar_url(url)
        local_path, filename = download_nar(resolved)
        try:
            print(f"[ci-deploy] Uploading NAR {filename} to runtime...", file=sys.stderr)
            with open(local_path, "rb") as f:
                body = f.read()
            nipyapi.extensions.upload_nar(file_bytes=body, filename=filename, timeout=150)
            print(f"[ci-deploy] NAR {filename} installed successfully", file=sys.stderr)
        finally:
            os.unlink(local_path)


def _isolate_parameter_contexts(flow_def, group_name):
    contexts = flow_def.get("parameterContexts", {})
    names = {name: f"{group_name} - {name}" for name in contexts}
    for name, context in contexts.items():
        context["name"] = names[name]
        if "inheritedParameterContexts" in context:
            context["inheritedParameterContexts"] = [names.get(inherited, inherited) for inherited in context["inheritedParameterContexts"]]
    if contexts:
        flow_def["parameterContexts"] = {names[name]: context for name, context in contexts.items()}

    def rename_references(group):
        name = group.get("parameterContextName")
        if name in names:
            group["parameterContextName"] = names[name]
        for child in group.get("processGroups", []):
            rename_references(child)

    rename_references(flow_def["flowContents"])


def upload_flow(flow_path, group_name=None):
    with open(flow_path, "r") as f:
        flow_def = json.load(f)
    group_name = group_name or f"CI_{os.path.basename(flow_path).removesuffix('.json')}_{uuid.uuid4().hex[:12]}"
    _isolate_parameter_contexts(flow_def, group_name)
    print(f"[ci-deploy] Flow name: {group_name}", file=sys.stderr)

    root_pg_id = get_root_pg_id()

    filename = os.path.basename(flow_path)
    file_tuple = (filename, json.dumps(flow_def).encode("utf-8"), "application/json")
    result = nipyapi.nifi.ProcessGroupsApi().upload_process_group(
        id=root_pg_id,
        file=file_tuple,
        group_name=group_name,
        position_x="0.0",
        position_y="0.0",
        client_id=str(uuid.uuid4()),
    )

    pg_id = result.id
    print(f"[ci-deploy] Uploaded PG: {pg_id}", file=sys.stderr)
    return pg_id, group_name


def deploy(flow_path, config_path, runtime_url, pat=None, group_name=None, output_file=None):
    with open(config_path) as f:
        config = yaml.safe_load(f)

    with open(os.path.join(os.path.dirname(__file__), "ci-runtime-schema.json")) as handle:
        validate(config, json.load(handle), format_checker=FormatChecker())
    nifi_auth = get_nifi_auth()
    configure_nifi(runtime_url, pat=pat, nifi_auth=nifi_auth)
    api_base = nipyapi.config.nifi_config.host

    nar_urls = config.get("nars", [])
    upload_nars(nar_urls)

    for registry in config.get("flow_registries", []):
        properties = {key: resolve_value(value) for key, value in registry.get("properties", {}).items()}
        setup_registry(registry["name"], properties, api_base, pat, type_override=registry.get("type"), nifi_auth=nifi_auth)
    if config.get("controller_services"):
        reconcile_controller_services(config["controller_services"], api_base, pat, nifi_auth=nifi_auth)

    pg_id, group_name = upload_flow(flow_path, group_name=group_name)
    if output_file:
        with open(output_file, "w") as handle:
            handle.write(pg_id)

    providers = config.get("parameter_providers", [])
    provider_context_names = []
    if providers:
        provider_context_names = reconcile_parameter_providers(providers, api_base, pat, nifi_auth=nifi_auth)

    if provider_context_names:
        pattern = config.get("provided_parameter_contexts")
        if pattern:
            filtered = [n for n in provider_context_names if re.fullmatch(pattern, n)]
            if filtered:
                print(f"[ci-deploy] Adding inherited contexts: {filtered}", file=sys.stderr)
                add_inherited_parameter_contexts(pg_id, filtered, pg_name=group_name)

    flow_config = config.get("flow", {})

    assets = flow_config.get("assets", [])
    if assets:
        reconcile_flow_assets(pg_id, assets, pg_name=group_name)

    parameters = flow_config.get("parameters", {})
    if parameters:
        resolved = {k: resolve_value(v) if v else v for k, v in parameters.items()}
        reconcile_flow_parameters(pg_id, resolved, pg_name=group_name)

    overrides = flow_config.get("parameter_overrides", {})
    if overrides:
        apply_parameter_overrides(pg_id, overrides, pg_name=group_name)

    print(f"[ci-deploy] Starting flow '{group_name}'...", file=sys.stderr)
    start_flow(pg_id, group_name)
    print(f"[ci-deploy] Flow started successfully", file=sys.stderr)

    return pg_id


def main():
    parser = argparse.ArgumentParser(description="Deploy a temporary test flow to an existing NiFi instance")
    parser.add_argument("--flow-path", required=True, help="Path to flow JSON")
    parser.add_argument("--config", required=True, help="Path to test YAML")
    parser.add_argument("--runtime-url", default=os.environ.get("NIFI_URL"), required=not os.environ.get("NIFI_URL"), help="NiFi URL")
    parser.add_argument("--token", default=os.environ.get("NIFI_TOKEN", ""), help="NiFi bearer token")
    parser.add_argument("--output-file", help="File to write PG ID to")
    args = parser.parse_args()

    try:
        pg_id = deploy(args.flow_path, args.config, args.runtime_url, args.token, output_file=args.output_file)
        print(pg_id)
    except Exception as exc:
        from safe_exceptions import format_safe_exception
        print(f"[ci-deploy] Failed: {format_safe_exception(exc)}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
