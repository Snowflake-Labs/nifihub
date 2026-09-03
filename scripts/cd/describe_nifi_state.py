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
import os
import sys

import nipyapi

from manage_flows import configure_nifi, list_process_groups
from manage_controller_services import list_controller_services, list_root_pg_controller_services
from manage_service_bindings import build_root_pg_service_uuid_to_name_map, describe_flow_service_bindings
from setup_registry_client import list_registry_clients
from safe_exceptions import format_safe_exception
import manage_parameter_providers  # noqa: F401 — triggers monkey patch


def list_parameter_providers():
    result = nipyapi.nifi.FlowApi().get_parameter_providers()
    return result.parameter_providers or []


def _registry_id_to_name(registries):
    return {rc.id: rc.component.name for rc in registries}


def _is_sensitive_value(val):
    if val is None:
        return False
    return val == "***" or "Sensitive value set" in str(val)


def _clean_properties(props):
    if not props:
        return {}
    return {k: v for k, v in props.items() if not _is_sensitive_value(v) and v is not None}


def _select_api_method(api, candidate_names):
    for candidate_name in candidate_names:
        method = getattr(api, candidate_name, None)
        if callable(method):
            return method
    return None


def _empty_root_parameter_context_state(root_pg_id, provider_context_names=None):
    return {
        "root_process_group_id": root_pg_id,
        "context": {"present": False, "id": None, "name": None},
        "inherited": [],
        "parameters": {},
        "provider_context_names": list(provider_context_names or []),
        "entries": [],
        "blocked": False,
        "messages": [],
    }


def _blocked_root_parameter_context_state(desired_spec, root_pg_id, context, message, provider_context_names=None, inherited=None, parameters=None):
    entries = []
    for asset_spec in (desired_spec or {}).get("assets", []) or []:
        entries.append({
            "name": asset_spec.get("name"),
            "parameter": asset_spec.get("parameter"),
            "status": "blocked",
            "asset_id": None,
            "missing_content": None,
            "messages": [message],
        })
    return {
        "root_process_group_id": root_pg_id,
        "context": context,
        "inherited": list(inherited or []),
        "parameters": dict(parameters or {}),
        "provider_context_names": list(provider_context_names or []),
        "entries": entries,
        "blocked": True,
        "messages": [message],
    }


def _list_provider_context_names():
    flow_api = nipyapi.nifi.FlowApi()
    pc_api = nipyapi.nifi.ParameterContextsApi()
    contexts = flow_api.get_parameter_contexts()
    provider_names = []

    for ctx_ref in (contexts.parameter_contexts or []):
        ctx_id = getattr(ctx_ref, "id", None)
        component = getattr(ctx_ref, "component", None)
        ctx_name = getattr(component, "name", None) or getattr(ctx_ref, "name", None)
        provider_cfg = getattr(component, "parameter_provider_configuration", None)
        if provider_cfg is None and ctx_id:
            try:
                full_context = pc_api.get_parameter_context(id=ctx_id)
            except Exception:
                full_context = None
            if full_context is not None:
                component = getattr(full_context, "component", None)
                ctx_name = getattr(component, "name", ctx_name)
                provider_cfg = getattr(component, "parameter_provider_configuration", None)
        if provider_cfg and ctx_name:
            provider_names.append(ctx_name)

    return provider_names


def _describe_root_parameter_context(desired_spec):
    root_pg_id = nipyapi.canvas.get_root_pg_id()
    if not desired_spec:
        return {}

    try:
        provider_context_names = _list_provider_context_names()
    except Exception:
        provider_context_names = []

    empty_state = _empty_root_parameter_context_state(root_pg_id, provider_context_names)
    empty_context = empty_state["context"]

    try:
        root_pg = nipyapi.nifi.ProcessGroupsApi().get_process_group(id=root_pg_id)
    except Exception as exc:
        return _blocked_root_parameter_context_state(
            desired_spec,
            root_pg_id,
            empty_context,
            f"Failed to inspect root process group: {format_safe_exception(exc)}",
            provider_context_names=provider_context_names,
        )

    direct_context = getattr(root_pg.component, "parameter_context", None)
    context = {
        "present": bool(direct_context),
        "id": getattr(direct_context, "id", None),
        "name": getattr(getattr(direct_context, "component", None), "name", None),
    }
    if not direct_context:
        return {
            "root_process_group_id": root_pg_id,
            "context": context,
            "inherited": [],
            "parameters": {},
            "provider_context_names": provider_context_names,
            "entries": [{
                "name": asset_spec.get("name"),
                "parameter": asset_spec.get("parameter"),
                "status": "missing_context",
                "asset_id": None,
                "missing_content": None,
                "messages": ["Root process group has no direct parameter context"],
            } for asset_spec in ((desired_spec or {}).get("assets", []) or [])],
            "blocked": False,
            "messages": [],
        }

    pc_api = nipyapi.nifi.ParameterContextsApi()
    try:
        parameter_context = pc_api.get_parameter_context(id=direct_context.id)
    except Exception as exc:
        return _blocked_root_parameter_context_state(
            desired_spec,
            root_pg_id,
            context,
            f"Failed to inspect direct root parameter context: {format_safe_exception(exc)}",
            provider_context_names=provider_context_names,
        )

    inherited_contexts = [
        getattr(getattr(inherited, "component", None), "name", None) or getattr(inherited, "name", None)
        for inherited in (parameter_context.component.inherited_parameter_contexts or [])
    ]
    inherited_contexts = [name for name in inherited_contexts if name]

    direct_parameters = {
        entity.parameter.name: entity.parameter
        for entity in (parameter_context.component.parameters or [])
    }
    parameters = {}
    for parameter_name, parameter in direct_parameters.items():
        if getattr(parameter, "sensitive", False):
            parameters[parameter_name] = "<sensitive>"
        else:
            parameters[parameter_name] = getattr(parameter, "value", None)

    get_assets = _select_api_method(pc_api, ["get_assets1", "get_assets"])
    if get_assets is None:
        return _blocked_root_parameter_context_state(
            desired_spec,
            root_pg_id,
            context,
            "Failed to inspect root parameter-context assets: unsupported NiFi API method",
            provider_context_names=provider_context_names,
            inherited=inherited_contexts,
            parameters=parameters,
        )
    try:
        assets_result = get_assets(context_id=direct_context.id)
    except Exception as exc:
        return _blocked_root_parameter_context_state(
            desired_spec,
            root_pg_id,
            context,
            f"Failed to inspect root parameter-context assets: {format_safe_exception(exc)}",
            provider_context_names=provider_context_names,
            inherited=inherited_contexts,
            parameters=parameters,
        )

    assets_by_name = {
        asset_entity.asset.name: asset_entity.asset
        for asset_entity in (assets_result.assets or [])
    }
    entries = []
    for asset_spec in ((desired_spec or {}).get("assets", []) or []):
        asset_name = asset_spec.get("name")
        parameter_name = asset_spec.get("parameter")
        entry = {
            "name": asset_name,
            "parameter": parameter_name,
            "status": "matched",
            "asset_id": None,
            "missing_content": None,
            "messages": [],
        }
        parameter = direct_parameters.get(parameter_name)
        asset = assets_by_name.get(asset_name)
        if asset is not None:
            entry["asset_id"] = getattr(asset, "id", None)
            entry["missing_content"] = bool(getattr(asset, "missing_content", False))
        if parameter is None:
            entry["status"] = "missing_parameter"
            entry["messages"].append("Direct parameter not found in root parameter context")
            entries.append(entry)
            continue

        if getattr(parameter, "sensitive", False):
            entry["status"] = "sensitive_parameter"
            entry["messages"].append("Direct parameter is sensitive and cannot reference a root asset")
            entries.append(entry)
            continue

        references = list(getattr(parameter, "referenced_assets", None) or [])
        if len(references) > 1:
            entry["status"] = "multiple_references"
            entry["messages"].append("Direct parameter references multiple assets")
            entries.append(entry)
            continue

        parameter_value = getattr(parameter, "value", None)
        if not references and parameter_value not in (None, ""):
            entry["status"] = "literal_parameter"
            entry["messages"].append("Direct parameter has a literal value and cannot be converted safely")
            entries.append(entry)
            continue

        if asset is None:
            entry["status"] = "missing_asset"
            entry["messages"].append("Declared asset is missing from the direct root parameter context")
            entries.append(entry)
            continue

        if entry["missing_content"]:
            entry["status"] = "missing_content"
            entry["messages"].append("Declared asset exists but its content is missing")

        if not references:
            entry["status"] = "missing_reference"
            entry["messages"].append("Direct parameter does not reference the declared asset")
            entries.append(entry)
            continue

        reference = references[0]
        if getattr(reference, "id", None) != getattr(asset, "id", None) or getattr(reference, "name", None) != asset_name:
            entry["status"] = "wrong_reference"
            entry["messages"].append("Direct parameter references a different asset")

        entries.append(entry)

    return {
        "root_process_group_id": root_pg_id,
        "context": context,
        "inherited": inherited_contexts,
        "parameters": parameters,
        "provider_context_names": provider_context_names,
        "entries": entries,
        "blocked": False,
        "messages": [],
    }


def get_flow_parameters(pg_id):
    api = nipyapi.nifi.ProcessGroupsApi()
    pg = api.get_process_group(id=pg_id)
    pc_ref = pg.component.parameter_context
    if not pc_ref:
        return {}

    pc_api = nipyapi.nifi.ParameterContextsApi()
    params = {}
    seen = set()

    def walk(cid):
        if cid in seen:
            return
        seen.add(cid)
        pc = pc_api.get_parameter_context(id=cid)
        for p in (pc.component.parameters or []):
            param = p.parameter
            if param.name not in params:
                if param.sensitive:
                    params[param.name] = "<sensitive>"
                else:
                    params[param.name] = param.value
        for inherited in (pc.component.inherited_parameter_contexts or []):
            walk(inherited.id)

    walk(pc_ref.id)
    return params


def describe_nifi_state(runtime_url, pat=None, nifi_auth=None, desired_flows=None, desired_root_parameter_context=None):
    configure_nifi(runtime_url, pat=pat, nifi_auth=nifi_auth)

    registries = list_registry_clients()
    reg_id_map = _registry_id_to_name(registries)

    cs_list = list_controller_services()
    controller_services = []
    for cs in cs_list:
        controller_services.append({
            "name": cs.component.name,
            "type": cs.component.type,
            "state": cs.component.state,
            "properties": _clean_properties(cs.component.properties),
        })

    root_pg_cs_list = list_root_pg_controller_services()
    root_pg_controller_services = []
    for cs in root_pg_cs_list:
        root_pg_controller_services.append({
            "id": cs.component.id,
            "name": cs.component.name,
            "type": cs.component.type,
            "state": cs.component.state,
            "properties": _clean_properties(cs.component.properties),
        })
    root_pg_service_uuid_to_name = build_root_pg_service_uuid_to_name_map(root_pg_controller_services)

    pp_list = list_parameter_providers()
    parameter_providers = []
    for pp in pp_list:
        parameter_providers.append({
            "name": pp.component.name,
            "type": pp.component.type,
            "properties": _clean_properties(pp.component.properties),
        })

    flow_registries = []
    for rc in registries:
        flow_registries.append({
            "name": rc.component.name,
            "type": rc.component.type,
            "properties": _clean_properties(rc.component.properties),
        })

    process_groups = list_process_groups()
    flows = []
    parameters = {}
    desired_flows_by_name = {
        flow["name"].upper(): flow for flow in (desired_flows or []) if flow.get("name")
    }

    for pg in process_groups:
        vci = pg.component.version_control_information
        flow_entry = {"name": pg.component.name}

        if vci:
            registry_name = reg_id_map.get(vci.registry_id, vci.registry_id)
            flow_entry["registry"] = registry_name
            flow_entry["bucket"] = vci.bucket_id or vci.bucket_name or ""
            flow_entry["flow"] = vci.flow_id or vci.flow_name or ""
            flow_entry["version"] = vci.version or ""
            flow_entry["state"] = vci.state or ""
        else:
            flow_entry["registry"] = ""
            flow_entry["bucket"] = ""
            flow_entry["flow"] = ""
            flow_entry["version"] = ""
            flow_entry["state"] = ""

        running_count = pg.running_count or 0
        stopped_count = pg.stopped_count or 0
        if running_count > 0 and stopped_count == 0:
            flow_entry["running"] = True
        else:
            flow_entry["running"] = False

        desired_flow = desired_flows_by_name.get(pg.component.name.upper(), {})
        if desired_flow.get("service_bindings"):
            flow_entry["service_bindings"] = describe_flow_service_bindings(
                pg.id,
                pg.component.name,
                desired_flow.get("service_bindings", []),
                root_pg_service_uuid_to_name,
            )

        flows.append(flow_entry)

        try:
            params = get_flow_parameters(pg.id)
            if params:
                parameters[pg.component.name] = params
        except Exception as e:
            print(f"[nifi] Could not get parameters for '{pg.component.name}': {e}", file=sys.stderr)

    return {
        "controller_services": controller_services,
        "root_pg_controller_services": root_pg_controller_services,
        "parameter_providers": parameter_providers,
        "flow_registries": flow_registries,
        "flows": flows,
        "parameters": parameters,
        "root_parameter_context": _describe_root_parameter_context(desired_root_parameter_context),
    }


def main():
    if len(sys.argv) < 3:
        print("Usage: describe_nifi_state.py <runtime_url> <pat>", file=sys.stderr)
        sys.exit(1)

    runtime_url = sys.argv[1]
    pat = sys.argv[2] if len(sys.argv) > 2 else os.environ.get("NIFI_RUNTIME_PAT", "")

    import json
    state = describe_nifi_state(runtime_url, pat)
    json.dump(state, sys.stdout, indent=2)
    print()


if __name__ == "__main__":
    main()