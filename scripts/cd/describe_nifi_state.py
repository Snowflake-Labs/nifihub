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
from manage_controller_services import list_controller_services
from setup_registry_client import list_registry_clients
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


def describe_nifi_state(runtime_url, pat=None, nifi_auth=None):
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

        flows.append(flow_entry)

        try:
            params = get_flow_parameters(pg.id)
            if params:
                parameters[pg.component.name] = params
        except Exception as e:
            print(f"[nifi] Could not get parameters for '{pg.component.name}': {e}", file=sys.stderr)

    return {
        "controller_services": controller_services,
        "parameter_providers": parameter_providers,
        "flow_registries": flow_registries,
        "flows": flows,
        "parameters": parameters,
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