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
import argparse
import json
import time

import nipyapi

from manage_flows import configure_nifi

_orig_ref_type_setter = nipyapi.nifi.ControllerServiceReferencingComponentDTO.reference_type.fset
def _patched_ref_type_setter(self, value):
    allowed = ['Processor', 'ControllerService', 'ReportingTask', 'FlowRegistryClient', 'ParameterProvider', 'FlowAnalysisRule']
    if value not in allowed:
        raise ValueError(f"Invalid value for `reference_type` ({value}), must be one of {allowed}")
    self._reference_type = value
nipyapi.nifi.ControllerServiceReferencingComponentDTO.reference_type = property(
    nipyapi.nifi.ControllerServiceReferencingComponentDTO.reference_type.fget,
    _patched_ref_type_setter,
)


def list_controller_services():
    api = nipyapi.nifi.FlowApi()
    result = api.get_controller_services_from_controller()
    return result.controller_services or []


def find_controller_service_by_name(name):
    for cs in list_controller_services():
        if cs.component.name == name:
            return cs
    return None


def _refresh(name):
    cs = find_controller_service_by_name(name)
    if cs is None:
        raise RuntimeError(f"Controller service '{name}' disappeared unexpectedly")
    return cs


def _set_state(cs, state):
    api = nipyapi.nifi.ControllerServicesApi()
    body = nipyapi.nifi.ControllerServiceRunStatusEntity(
        revision=cs.revision,
        state=state,
    )
    api.update_run_status1(id=cs.id, body=body)
    print(f"[cs] '{cs.component.name}' -> {state}")
    time.sleep(2)
    return _refresh(cs.component.name)


def _create(svc_spec):
    api = nipyapi.nifi.ControllerApi()
    body = nipyapi.nifi.ControllerServiceEntity(
        revision=nipyapi.nifi.RevisionDTO(version=0),
        component=nipyapi.nifi.ControllerServiceDTO(
            name=svc_spec["name"],
            type=svc_spec["type"],
            properties=svc_spec.get("properties", {}),
        ),
    )
    result = api.create_controller_service(body=body)
    print(f"[cs] Created '{svc_spec['name']}' (id={result.id})")
    return result


def _update_properties(cs, desired_props):
    api = nipyapi.nifi.ControllerServicesApi()
    body = nipyapi.nifi.ControllerServiceEntity(
        id=cs.id,
        revision=cs.revision,
        component=nipyapi.nifi.ControllerServiceDTO(
            id=cs.id,
            properties=desired_props,
        ),
    )
    result = api.update_controller_service(id=cs.id, body=body)
    print(f"[cs] Updated properties for '{cs.component.name}'")
    return result


def _properties_match(cs, desired_props):
    current = cs.component.properties or {}
    return all(current.get(k) == v for k, v in desired_props.items())


def reconcile_controller_services(services, runtime_url, nifi_pat, nifi_auth=None):
    """Idempotent reconcile: create missing services, update mismatched properties, ensure all are ENABLED."""
    configure_nifi(runtime_url, pat=nifi_pat, nifi_auth=nifi_auth)
    for svc_spec in services:
        name = svc_spec["name"]
        desired_props = svc_spec.get("properties", {})
        cs = find_controller_service_by_name(name)

        if not cs:
            cs = _create(svc_spec)
            cs = _refresh(name)
        else:
            if not _properties_match(cs, desired_props):
                if cs.component.state == "ENABLED":
                    cs = _set_state(cs, "DISABLED")
                cs = _update_properties(cs, desired_props)
                cs = _refresh(name)
            else:
                print(f"[cs] '{name}' properties up-to-date")

        if cs.component.state != "ENABLED":
            _set_state(cs, "ENABLED")
        else:
            print(f"[cs] '{name}' already ENABLED")


def delete_controller_services(services, runtime_url, nifi_pat, nifi_auth=None):
    """Disable and delete controller services."""
    configure_nifi(runtime_url, pat=nifi_pat, nifi_auth=nifi_auth)
    for svc_spec in services:
        name = svc_spec["name"]
        cs = find_controller_service_by_name(name)
        if not cs:
            print(f"[cs] '{name}' not found, skipping delete")
            continue
        if cs.component.state == "ENABLED":
            cs = _set_state(cs, "DISABLED")
        api = nipyapi.nifi.ControllerServicesApi()
        api.remove_controller_service(id=cs.id, version=str(cs.revision.version))
        print(f"[cs] Deleted '{name}'")


def main():
    parser = argparse.ArgumentParser(description="Manage controller services on NiFi runtime")
    parser.add_argument("action", choices=["reconcile", "delete"])
    parser.add_argument("--services", required=True, help="JSON array of controller service specs")
    parser.add_argument("--runtime-url", required=True)
    parser.add_argument("--nifi-pat", required=True)
    args = parser.parse_args()
    services = json.loads(args.services)
    if args.action == "reconcile":
        reconcile_controller_services(services, args.runtime_url, args.nifi_pat)
    elif args.action == "delete":
        delete_controller_services(services, args.runtime_url, args.nifi_pat)


if __name__ == "__main__":
    main()