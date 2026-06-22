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
"""Deploy a flow from JSON to an ephemeral CI runtime with full CD-style setup.

Uploads the flow JSON, adds inherited parameter contexts from providers,
applies parameters and assets, then starts the flow.

Usage:
    python scripts/ci/deploy_ci_flow.py \
        --flow-path flows/data-generator/postgres-cdc-demo.json \
        --config flows/data-generator/tests/test_postgres_cdc_demo.yaml \
        --runtime-url https://of--account.snowflakecomputing.app/key/nifi-api \
        --pat <token> \
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

from manage_flows import configure_nifi, find_flow_pg_by_name, start_flow, get_root_pg_id
from manage_parameters import reconcile_flow_parameters, add_inherited_parameter_contexts, resolve_value, apply_parameter_overrides
from manage_assets import reconcile_flow_assets
from manage_parameter_providers import reconcile_parameter_providers, fetch_auto_provisioned_provider
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


def upload_flow(flow_path):
    with open(flow_path, "r") as f:
        flow_def = json.load(f)
    group_name = flow_def.get("flowContents", {}).get("name", os.path.basename(flow_path))
    print(f"[ci-deploy] Flow name: {group_name}", file=sys.stderr)

    root_pg_id = get_root_pg_id()

    filename = os.path.basename(flow_path)
    with open(flow_path, "rb") as fh:
        file_tuple = (filename, fh.read(), "application/json")
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


def deploy(flow_path, config_path, runtime_url, pat):
    with open(config_path) as f:
        config = yaml.safe_load(f)

    base = re.sub(r"/nifi-api/?$", "", re.sub(r"/nifi/?$", "", runtime_url.rstrip("/")))
    api_base = base + "/nifi-api"

    nifi_auth_cfg = config.get("nifi_auth")
    nifi_auth = None
    if nifi_auth_cfg and nifi_auth_cfg.get("type") == "username_password":
        from manage_parameters import resolve_value
        nifi_auth = {
            "type": "username_password",
            "username": resolve_value(nifi_auth_cfg["username"]),
            "password": resolve_value(nifi_auth_cfg["password"]),
            "verify_ssl": nifi_auth_cfg.get("verify_ssl", True),
        }

    configure_nifi(api_base, pat=pat, nifi_auth=nifi_auth)

    nar_urls = config.get("nars", [])
    upload_nars(nar_urls)

    pg_id, group_name = upload_flow(flow_path)

    providers = config.get("parameter_providers", [])
    provider_context_names = []
    if providers:
        provider_context_names = reconcile_parameter_providers(providers, api_base, pat, nifi_auth=nifi_auth)

    sensitive_pattern = config.get("sensitive_param_pattern", ".*")
    auto_names = fetch_auto_provisioned_provider(sensitive_pattern=sensitive_pattern)
    if auto_names:
        provider_context_names.extend(auto_names)

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
    parser = argparse.ArgumentParser(description="Deploy a flow to CI runtime with full setup")
    parser.add_argument("--flow-path", required=True, help="Path to flow JSON")
    parser.add_argument("--config", required=True, help="Path to test YAML")
    parser.add_argument("--runtime-url", required=True, help="Runtime NiFi API URL")
    parser.add_argument("--pat", required=True, help="NiFi PAT")
    parser.add_argument("--output-file", help="File to write PG ID to")
    args = parser.parse_args()

    try:
        pg_id = deploy(args.flow_path, args.config, args.runtime_url, args.pat)
        if args.output_file:
            with open(args.output_file, "w") as f:
                f.write(pg_id)
        print(pg_id)
    except Exception as exc:
        print(f"[ci-deploy] Failed: {exc}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()