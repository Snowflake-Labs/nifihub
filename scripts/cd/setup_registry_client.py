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
import sys

import nipyapi

from manage_flows import configure_nifi as _configure_nifi


def list_registry_clients():
    api = nipyapi.nifi.ControllerApi()
    result = api.get_flow_registry_clients()
    return result.registries or []


def find_registry_client(name):
    for rc in list_registry_clients():
        if rc.component.name == name:
            return rc
    return None


def _resolve_type(type_override):
    """Return type_override if set, otherwise auto-detect first GitHub/Git-based type."""
    if type_override:
        return type_override
    api = nipyapi.nifi.ControllerApi()
    descriptor_result = api.get_registry_client_types()
    all_types = [t.type for t in (descriptor_result.flow_registry_client_types or [])]
    print(f"[registry] Available Flow Registry Client types: {all_types}")
    for frc_type in all_types:
        if "github" in frc_type.lower():
            return frc_type
    for frc_type in all_types:
        if "git" in frc_type.lower():
            return frc_type
    raise RuntimeError("Git-based Flow Registry Client type not found on runtime")


def create_registry_client(name, properties, type_override=None):
    api = nipyapi.nifi.ControllerApi()
    client_type = _resolve_type(type_override)

    body = nipyapi.nifi.FlowRegistryClientEntity(
        component=nipyapi.nifi.FlowRegistryClientDTO(
            name=name,
            type=client_type,
            properties=properties,
        ),
        revision=nipyapi.nifi.RevisionDTO(version=0),
    )

    print(f"[registry] Creating Flow Registry Client '{name}' (type={client_type})...")
    result = api.create_flow_registry_client(body=body)
    print(f"[registry] Created Flow Registry Client '{name}' (id={result.id})")
    return result


def delete_registry_client(existing):
    api = nipyapi.nifi.ControllerApi()
    print(f"[registry] Deleting Flow Registry Client '{existing.component.name}' (id={existing.id})...")
    api.delete_flow_registry_client(
        id=existing.id,
        version=existing.revision.version,
    )
    print(f"[registry] Deleted Flow Registry Client '{existing.component.name}'")


def update_registry_client(existing, properties):
    api = nipyapi.nifi.ControllerApi()
    full_props = {k: None for k in (existing.component.properties or {})}
    full_props.update(properties)
    existing.component.properties = full_props
    print(f"[registry] Updating Flow Registry Client '{existing.component.name}' (nulling {[k for k, v in full_props.items() if v is None]})...")
    result = api.update_flow_registry_client(
        id=existing.id,
        body=existing,
    )
    print(f"[registry] Updated Flow Registry Client '{existing.component.name}'")
    return result


def setup(name, properties, runtime_url, nifi_pat, type_override=None, nifi_auth=None):
    """Provision or update a Flow Registry Client on the given runtime.

    If the existing client has a different type, it is deleted and recreated
    (NiFi does not allow changing the type of an existing registry client).

    Args:
        name: Registry client name in NiFi.
        properties: Dict of key-value properties for the API (sensitive values
            such as PAT must be injected by the caller before passing here).
        runtime_url: NiFi runtime base URL.
        nifi_pat: Bearer token for NiFi API authentication.
        type_override: Optional fully-qualified Java type. Auto-detected if None.
        nifi_auth: Optional nifi_auth dict for username/password auth.
    """
    _configure_nifi(runtime_url, pat=nifi_pat, nifi_auth=nifi_auth)
    desired_type = _resolve_type(type_override)
    existing = find_registry_client(name)
    if existing:
        if existing.component.type != desired_type:
            print(
                f"[registry] Type mismatch for '{name}': "
                f"existing={existing.component.type}, desired={desired_type}. "
                f"Deleting and recreating."
            )
            delete_registry_client(existing)
            return create_registry_client(name, properties, desired_type)
        return update_registry_client(existing, properties)
    return create_registry_client(name, properties, desired_type)


def main():
    parser = argparse.ArgumentParser(description="Setup Flow Registry Client on NiFi runtime")
    parser.add_argument("--name", default="nifihub", help="Registry client name")
    parser.add_argument("--type", dest="type_override", default=None,
                        help="Fully-qualified Java type (auto-detected if omitted)")
    parser.add_argument("--properties", required=True,
                        help="JSON object of key-value properties for the registry client")
    parser.add_argument("--runtime-url", required=True, help="NiFi runtime URL")
    parser.add_argument("--nifi-pat", required=True, help="PAT for NiFi API auth")
    args = parser.parse_args()

    properties = json.loads(args.properties)
    result = setup(
        args.name, properties, args.runtime_url, args.nifi_pat,
        type_override=args.type_override,
    )
    json.dump({"id": result.id, "name": result.component.name}, sys.stdout, indent=2)


if __name__ == "__main__":
    main()