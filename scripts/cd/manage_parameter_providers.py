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
import re
import time

import nipyapi

from manage_controller_services import list_controller_services
from manage_flows import configure_nifi

_ParameterGroupCE = nipyapi.nifi.models.ParameterGroupConfigurationEntity
_orig_ps_setter = _ParameterGroupCE.parameter_sensitivities.fset

def _patched_ps_setter(self, value):
    self._parameter_sensitivities = value

_ParameterGroupCE.parameter_sensitivities = _ParameterGroupCE.parameter_sensitivities.setter(_patched_ps_setter)


def _build_cs_map():
    """Return {cs_name: cs_id} for all existing controller-level services."""
    return {cs.component.name: cs.component.id for cs in list_controller_services()}


def find_parameter_provider(name):
    result = nipyapi.nifi.FlowApi().get_parameter_providers()
    for pp in (result.parameter_providers or []):
        if pp.component.name == name:
            return pp
    return None


def _create(name, type_str, properties):
    body = nipyapi.nifi.ParameterProviderEntity(
        component=nipyapi.nifi.ParameterProviderDTO(
            name=name,
            type=type_str,
            properties=properties,
        ),
        revision=nipyapi.nifi.RevisionDTO(version=0),
    )
    result = nipyapi.nifi.ControllerApi().create_parameter_provider(body)
    print(f"[pp] Created '{name}' (id={result.id})")
    return result


def _properties_match(pp, desired_props):
    current = pp.component.properties or {}
    return all(current.get(k) == v for k, v in desired_props.items())


def _update(pp, properties):
    body = nipyapi.nifi.ParameterProviderEntity(
        id=pp.id,
        revision=pp.revision,
        component=nipyapi.nifi.ParameterProviderDTO(
            id=pp.component.id,
            properties=properties,
        ),
    )
    result = nipyapi.nifi.ParameterProvidersApi().update_parameter_provider(body, pp.id)
    print(f"[pp] Updated '{pp.component.name}'")
    return result


def _fetch(pp):
    """Synchronously fetch parameters from the provider. Returns updated entity."""
    body = nipyapi.nifi.ParameterProviderParameterFetchEntity(
        id=pp.id,
        revision=pp.revision,
    )
    result = nipyapi.nifi.ParameterProvidersApi().fetch_parameters(body, pp.id)
    groups = result.component.parameter_group_configurations or []
    print(f"[pp] Fetched {len(groups)} parameter group(s)")
    return result


def _apply(provider_id, sensitive_pattern):
    """Apply fetched parameter groups, classifying sensitivities."""
    api = nipyapi.nifi.ParameterProvidersApi()
    pp = api.get_parameter_provider(provider_id)
    groups = pp.component.parameter_group_configurations or []
    if not groups:
        print(f"[pp] No parameter groups to apply")
        return

    pattern = re.compile(sensitive_pattern)
    for group in groups:
        sensitivities = dict(group.parameter_sensitivities or {})
        for pname in sensitivities:
            sensitivities[pname] = "SENSITIVE" if pattern.fullmatch(pname) else "NON_SENSITIVE"
        group.parameter_sensitivities = sensitivities
        group.synchronized = True
        if not group.parameter_context_name:
            group.parameter_context_name = group.group_name

    body = nipyapi.nifi.ParameterProviderParameterApplicationEntity(
        id=provider_id,
        revision=pp.revision,
        parameter_group_configurations=groups,
    )
    context_names = [g.parameter_context_name for g in groups]

    req = api.submit_apply_parameters(body, provider_id)
    request_id = req.request.request_id
    print(f"[pp] Applying parameters...")
    while True:
        time.sleep(1)
        status = api.get_parameter_provider_apply_parameters_request(provider_id, request_id)
        if status.request.complete:
            api.delete_apply_parameters_request(provider_id, request_id)
            if status.request.failure_reason:
                raise RuntimeError(f"[pp] Apply failed: {status.request.failure_reason}")
            print(f"[pp] Parameters applied successfully")
            return context_names


def reconcile_parameter_provider(pp_spec):
    name = pp_spec["name"]
    type_str = pp_spec["type"]
    sensitive_pattern = pp_spec.get("sensitive_param_pattern", ".*")

    cs_map = _build_cs_map()
    properties = {k: cs_map.get(v, v) for k, v in pp_spec.get("properties", {}).items()}

    existing = find_parameter_provider(name)
    if not existing:
        pp = _create(name, type_str, properties)
    elif not _properties_match(existing, properties):
        pp = _update(existing, properties)
    else:
        print(f"[pp] '{name}' properties up-to-date")
        pp = existing

    _fetch(pp)
    return _apply(pp.id, sensitive_pattern)


def reconcile_parameter_providers(pp_specs, runtime_url, nifi_pat, nifi_auth=None):
    configure_nifi(runtime_url, pat=nifi_pat, nifi_auth=nifi_auth)
    all_context_names = []
    for pp_spec in pp_specs:
        names = reconcile_parameter_provider(pp_spec)
        if names:
            all_context_names.extend(names)
    return all_context_names


AUTO_PROVISIONED_PROVIDER_NAME = "Openflow - Snowflake Parameter Provider"


def fetch_auto_provisioned_provider(sensitive_pattern=".*"):
    """Fetch and apply the auto-provisioned Snowflake Parameter Provider.

    This provider exists on every runtime by default. Fetching it discovers
    Snowflake secrets and creates parameter contexts that are then added as
    inherited to all flows.

    Returns list of parameter context names created by the provider.
    """
    pp = find_parameter_provider(AUTO_PROVISIONED_PROVIDER_NAME)
    if not pp:
        print(f"[pp] Auto-provisioned provider '{AUTO_PROVISIONED_PROVIDER_NAME}' not found — skipping")
        return []
    print(f"[pp] Fetching auto-provisioned provider '{AUTO_PROVISIONED_PROVIDER_NAME}'...")
    _fetch(pp)
    return _apply(pp.id, sensitive_pattern) or []


def delete_parameter_providers(pp_specs, runtime_url, nifi_pat, nifi_auth=None):
    configure_nifi(runtime_url, pat=nifi_pat, nifi_auth=nifi_auth)
    api = nipyapi.nifi.ParameterProvidersApi()
    for pp_spec in pp_specs:
        name = pp_spec["name"]
        pp = find_parameter_provider(name)
        if not pp:
            print(f"[pp] '{name}' not found, skipping delete")
            continue
        # NiFi refuses to delete a provider that is still referenced by
        # parameter contexts. Delete those contexts first.
        # These are "provided" contexts — auto-created when the provider
        # applied its parameter groups. Deleting them is safe as long as
        # they are not referenced by any process group as its active context.
        try:
            import time as _time
            flow_api = nipyapi.nifi.FlowApi()
            pc_api = nipyapi.nifi.ParameterContextsApi()
            all_contexts = flow_api.get_parameter_contexts()
            for ctx_ref in (all_contexts.parameter_contexts or []):
                ctx = pc_api.get_parameter_context(id=ctx_ref.id)
                cfg = ctx.component.parameter_provider_configuration
                # cfg is a ParameterProviderConfigurationEntity; the provider
                # id is at cfg.id (the entity id), not cfg.parameter_provider_id
                if cfg and cfg.id == pp.id:
                    try:
                        pc_api.delete_parameter_context(
                            id=ctx.id,
                            version=str(ctx.revision.version),
                        )
                        print(f"[pp] Deleted provided context '{ctx.component.name}'")
                    except Exception as ctx_err:
                        # Context may be in use by a process group — skip
                        print(f"[pp] Could not delete context '{ctx.component.name}': {ctx_err}")
        except Exception as e:
            print(f"[pp] Warning: could not clean up contexts for '{name}': {e}")
        # Re-fetch to get latest revision before deleting
        pp = find_parameter_provider(name)
        if not pp:
            continue
        api.remove_parameter_provider(pp.id, version=str(pp.revision.version))
        print(f"[pp] Deleted '{name}'")