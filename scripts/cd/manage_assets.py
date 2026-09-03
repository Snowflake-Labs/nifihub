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
import urllib.parse
import urllib.request
import ssl
import time
from collections import defaultdict

import certifi
import nipyapi

from manage_flows import configure_nifi
from safe_exceptions import format_safe_exception


PARAMETER_CONTEXT_UPDATE_TIMEOUT_SECONDS = 60
PARAMETER_CONTEXT_UPDATE_POLL_INTERVAL_SECONDS = 1
ASSET_DOWNLOAD_TIMEOUT_SECONDS = 60
ASSET_DOWNLOAD_MAX_BYTES = 256 * 1024 * 1024
_VALUE_UNSET = object()


def _select_api_method(api, candidate_names, api_name, operation_name):
    for candidate_name in candidate_names:
        method = getattr(api, candidate_name, None)
        if callable(method):
            return method
    names = ", ".join(candidate_names)
    raise AttributeError(f"{api_name} does not provide a supported {operation_name} method ({names})")


def _is_revision_conflict(exc):
    if getattr(exc, "status", None) != 409:
        return False
    reason = str(getattr(exc, "reason", "") or "").lower()
    body = str(getattr(exc, "body", "") or "").lower()
    message = str(exc).lower()

    def has_revision_conflict_marker(text):
        return (
            ("revision" in text and "conflict" in text)
            or "current revision" in text
            or "not the most up-to-date revision" in text
        )

    return (
        has_revision_conflict_marker(body)
        or has_revision_conflict_marker(message)
        or ("revision" in reason and "conflict" in reason)
    )


def _entity_id(entity):
    component = getattr(entity, "component", None)
    return getattr(entity, "id", None) or getattr(component, "id", None)


def _component_name(entity):
    component = getattr(entity, "component", None)
    return getattr(component, "name", None) or ""


def _copy_parameter(param, referenced_assets=None, value_marker=_VALUE_UNSET):
    kwargs = {
        "name": param.name,
        "sensitive": getattr(param, "sensitive", False),
        "description": getattr(param, "description", None),
    }
    if referenced_assets is not None:
        kwargs["referenced_assets"] = referenced_assets
    else:
        kwargs["referenced_assets"] = list(getattr(param, "referenced_assets", None) or [])
    if value_marker is not _VALUE_UNSET:
        kwargs["value"] = value_marker
    elif hasattr(param, "value"):
        kwargs["value"] = getattr(param, "value", None)
    return nipyapi.nifi.ParameterDTO(**kwargs)


def _build_param_map(pc_id):
    api = nipyapi.nifi.ParameterContextsApi()
    result = {}
    seen = set()

    def walk(cid):
        if cid in seen:
            return
        seen.add(cid)
        pc = api.get_parameter_context(id=cid)
        for p in (pc.component.parameters or []):
            param = p.parameter
            if param.name not in result:
                result[param.name] = (cid, param)
        for inherited in (pc.component.inherited_parameter_contexts or []):
            walk(inherited.id)

    walk(pc_id)
    return result


def _validate_asset_specs(assets_spec, scope_name):
    duplicate_names = sorted(name for name, count in _count_values(asset["name"] for asset in assets_spec).items() if count > 1)
    duplicate_parameters = sorted(name for name, count in _count_values(asset["parameter"] for asset in assets_spec).items() if count > 1)
    if duplicate_names:
        raise RuntimeError(f"[assets] Duplicate asset names declared for {scope_name}: {', '.join(duplicate_names)}")
    if duplicate_parameters:
        raise RuntimeError(f"[assets] Multiple assets declared for the same parameter in {scope_name}: {', '.join(duplicate_parameters)}")


def _count_values(values):
    counts = defaultdict(int)
    for value in values:
        counts[value] += 1
    return counts


def _get_existing_assets(context_id):
    api = nipyapi.nifi.ParameterContextsApi()
    get_assets = _select_api_method(
        api,
        ["get_assets1", "get_assets"],
        "ParameterContextsApi",
        "asset listing",
    )
    result = get_assets(context_id=context_id)
    assets = {}
    for asset_entity in (result.assets or []):
        asset = asset_entity.asset
        assets[asset.name] = asset
    return assets


def _download_file(url):
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme.lower() not in {"http", "https"}:
        raise RuntimeError("[assets] Asset URL must use http or https")
    print(f"[assets] Downloading '{parsed.path.rsplit('/', 1)[-1] or '<asset>'}' ...")
    req = urllib.request.Request(url, headers={"User-Agent": "nifihub-cd/1.0"})
    try:
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        with urllib.request.urlopen(req, timeout=ASSET_DOWNLOAD_TIMEOUT_SECONDS, context=ssl_context) as resp:
            final_url = resp.geturl()
            final_scheme = urllib.parse.urlparse(final_url).scheme.lower()
            if final_scheme not in {"http", "https"}:
                raise RuntimeError("[assets] Asset redirect URL must use http or https")
            content_length = resp.headers.get("Content-Length")
            if content_length and int(content_length) > ASSET_DOWNLOAD_MAX_BYTES:
                raise RuntimeError(
                    f"[assets] Asset exceeds the {ASSET_DOWNLOAD_MAX_BYTES}-byte download limit"
                )
            data = resp.read(ASSET_DOWNLOAD_MAX_BYTES + 1)
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"[assets] Asset download failed: {format_safe_exception(exc)}") from None
    if len(data) > ASSET_DOWNLOAD_MAX_BYTES:
        raise RuntimeError(f"[assets] Asset exceeds the {ASSET_DOWNLOAD_MAX_BYTES}-byte download limit")
    print(f"[assets] Downloaded {len(data)} bytes")
    return data


def _upload_asset(context_id, filename, data):
    api = nipyapi.nifi.ParameterContextsApi()
    create_asset = _select_api_method(
        api,
        ["create_asset1", "create_asset"],
        "ParameterContextsApi",
        "asset creation",
    )
    try:
        result = create_asset(body=data, context_id=context_id, filename=filename)
    except Exception as exc:
        raise RuntimeError(
            f"[assets] Failed to upload asset '{filename}' to context '{context_id}': {format_safe_exception(exc)}"
        ) from None
    asset_id = result.asset.id
    print(f"[assets] Uploaded asset '{filename}' -> id={asset_id}")
    return result.asset


def _submit_parameter_context_update(context_id, body, action, timeout=PARAMETER_CONTEXT_UPDATE_TIMEOUT_SECONDS, interval=PARAMETER_CONTEXT_UPDATE_POLL_INTERVAL_SECONDS):
    api = nipyapi.nifi.ParameterContextsApi()
    request_id = None
    try:
        req = api.submit_parameter_context_update(context_id=context_id, body=body)
        request_id = req.request.request_id
    except Exception as exc:
        raise RuntimeError(
            f"[assets] Failed to submit parameter context update for {action}: {format_safe_exception(exc)}"
        ) from None

    deadline = time.time() + timeout
    try:
        while True:
            status = api.get_parameter_context_update(context_id=context_id, request_id=request_id)
            request = status.request
            if request.complete:
                if request.failure_reason:
                    raise RuntimeError(f"[assets] Parameter context update failed for {action}: {request.failure_reason}")
                return status
            if time.time() >= deadline:
                raise RuntimeError(f"[assets] Parameter context update timed out for {action} after {timeout}s")
            time.sleep(interval)
    finally:
        if request_id:
            try:
                api.delete_update_request(context_id=context_id, request_id=request_id)
            except Exception:
                pass


def _update_direct_parameter_context(context_id, mutate_fn, action):
    api = nipyapi.nifi.ParameterContextsApi()
    pc = api.get_parameter_context(id=context_id)
    current = {p.parameter.name: p.parameter for p in (pc.component.parameters or [])}
    updated = mutate_fn(current)
    body = nipyapi.nifi.ParameterContextEntity(
        id=context_id,
        revision=pc.revision,
        component=nipyapi.nifi.ParameterContextDTO(
            id=context_id,
            name=pc.component.name,
            parameters=[nipyapi.nifi.ParameterEntity(parameter=param) for param in updated],
            inherited_parameter_contexts=list(pc.component.inherited_parameter_contexts or []),
        ),
    )
    _submit_parameter_context_update(context_id, body, action)
    refreshed = api.get_parameter_context(id=context_id)
    direct = {p.parameter.name: p.parameter for p in (refreshed.component.parameters or [])}
    return refreshed, direct


def _bind_asset_to_parameter(context_id, param_name, asset_id, asset_name):
    def mutate(current):
        param = current.get(param_name)
        if param is None:
            raise RuntimeError(f"[assets] Parameter '{param_name}' disappeared before asset binding")
        print(f"[assets] Binding parameter '{param_name}' -> asset '{asset_name}' (id={asset_id})")
        return [
            _copy_parameter(
                param,
                referenced_assets=[nipyapi.nifi.AssetReferenceDTO(id=asset_id, name=asset_name)],
                value_marker=None,
            )
        ]

    _update_direct_parameter_context(
        context_id,
        mutate,
        f"bind parameter '{param_name}' to asset '{asset_name}'",
    )
    print(f"[assets] Parameter '{param_name}' bound to asset '{asset_name}'")


def _reconcile_asset_reference(context_id, param_name, param_dto, asset_name, asset_url):
    existing = _get_existing_assets(context_id)
    existing_asset = existing.get(asset_name)

    if existing_asset and not existing_asset.missing_content:
        print(f"[assets] Asset '{asset_name}' already exists (id={existing_asset.id}) — skipping upload")
        refs = list(getattr(param_dto, "referenced_assets", None) or [])
        if refs and refs[0].id == existing_asset.id and refs[0].name == asset_name:
            print(f"[assets] Parameter '{param_name}' already bound to asset '{asset_name}'")
            return
        _bind_asset_to_parameter(context_id, param_name, existing_asset.id, asset_name)
        return

    data = _download_file(asset_url)
    asset = _upload_asset(context_id, asset_name, data)
    _bind_asset_to_parameter(context_id, param_name, asset.id, asset_name)


def reconcile_flow_assets(pg_id, assets_spec, pg_name=""):
    if not assets_spec:
        return

    _validate_asset_specs(assets_spec, f"flow '{pg_name or pg_id}'")

    pg = nipyapi.nifi.ProcessGroupsApi().get_process_group(id=pg_id)
    pc_ref = pg.component.parameter_context
    if not pc_ref:
        print(f"[assets] '{pg_name or pg_id}' has no parameter context — skipping")
        return

    param_map = _build_param_map(pc_ref.id)

    for asset_spec in assets_spec:
        asset_name = asset_spec["name"]
        asset_url = asset_spec["url"]
        param_name = asset_spec["parameter"]

        if param_name not in param_map:
            print(f"[assets] WARNING: parameter '{param_name}' not found in context of '{pg_name}' — skipping asset '{asset_name}'")
            continue

        context_id, param_dto = param_map[param_name]
        _reconcile_asset_reference(context_id, param_name, param_dto, asset_name, asset_url)


def reconcile_assets(flows_with_assets, runtime_url, nifi_pat):
    configure_nifi(runtime_url, nifi_pat)
    for flow_spec, pg_id in flows_with_assets:
        assets = flow_spec.get("assets")
        if not assets:
            continue
        print(f"[assets] Reconciling assets for '{flow_spec['name']}'...")
        reconcile_flow_assets(pg_id, assets, pg_name=flow_spec["name"])