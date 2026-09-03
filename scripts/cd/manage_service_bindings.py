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
from collections import defaultdict

import nipyapi

from manage_controller_services import list_root_pg_controller_services
from manage_flows import configure_nifi, list_process_groups, start_flow


_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE)
_ABSENT = object()


def _get(obj, *names, default=None):
    for name in names:
        if obj is None:
            return default
        if isinstance(obj, dict) and name in obj:
            return obj[name]
        if hasattr(obj, name):
            return getattr(obj, name)
    return default


def _component(entity):
    return _get(entity, "component")


def _entity_id(entity):
    component = _component(entity)
    return _get(entity, "id") or _get(component, "id")


def _component_name(entity):
    return _get(_component(entity), "name", default="")


def _component_parent_group_id(entity):
    return _get(_component(entity), "parent_group_id", "parentGroupId")


def _component_type(entity):
    return _get(_component(entity), "type", default="")


def _component_state(entity):
    component = _component(entity)
    return _get(component, "state", default="")


def _processor_run_status(entity):
    status = _get(entity, "status")
    return _get(status, "run_status", "runStatus", default="") or _component_state(entity)


def _processor_properties(entity):
    config = _get(_component(entity), "config")
    return dict(_get(config, "properties", default={}) or {})


def _processor_descriptors(entity):
    config = _get(_component(entity), "config")
    return dict(_get(config, "descriptors", default={}) or {})


def _controller_service_properties(entity):
    return dict(_get(_component(entity), "properties", default={}) or {})


def _controller_service_descriptors(entity):
    return dict(_get(_component(entity), "descriptors", default={}) or {})


def _validation_status(entity):
    component = _component(entity)
    return _get(component, "validation_status", "validationStatus", default="")


def _validation_errors(entity):
    component = _component(entity)
    errors = _get(component, "validation_errors", "validationErrors", default=None)
    return list(errors or [])


def _descriptor_identifies_controller_service(descriptor):
    return _get(descriptor, "identifies_controller_service", "identifiesControllerService", default="")


def _descriptor_sensitive(descriptor):
    return bool(_get(descriptor, "sensitive", default=False))


def _descriptor_required(descriptor):
    return bool(_get(descriptor, "required", default=False))


def _descriptor_dynamic(descriptor):
    return bool(_get(descriptor, "dynamic", default=False))


def _build_model(name, **kwargs):
    model = getattr(nipyapi.nifi, name)
    return model(**kwargs)


def _build_run_status_entity(entity, state, preferred_names):
    for name in preferred_names:
        model = getattr(nipyapi.nifi, name, None)
        if model is not None:
            return model(revision=_get(entity, "revision"), state=state)
    raise AttributeError(f"No supported run-status entity found for {preferred_names}")


def _select_api_method(api, candidate_names, api_name):
    for candidate_name in candidate_names:
        method = getattr(api, candidate_name, None)
        if callable(method):
            return method, candidate_name
    names = ", ".join(candidate_names)
    raise AttributeError(f"{api_name} does not provide a supported method ({names})")


def _controller_service_target_summary(candidate):
    return f"{candidate['kind']} '{candidate['name']}' in {candidate['process_group_path']}"


def _quiesce_flow(pg_id, flow_name):
    print(f"[bindings] Quiescing flow '{flow_name}' before applying service binding drift")
    nipyapi.canvas.schedule_process_group(pg_id, False)
    deadline = time.time() + 30
    while True:
        pg = nipyapi.nifi.ProcessGroupsApi().get_process_group(id=pg_id)
        running_count = int(_get(pg, "running_count", "runningCount", default=0) or 0)
        if running_count == 0:
            return pg
        if time.time() >= deadline:
            raise RuntimeError(f"Flow '{flow_name}' did not quiesce within 30s")
        time.sleep(1)


def _property_value_repr(value):
    if value is None:
        return "<unset>"
    if _UUID_RE.match(str(value)):
        return "<controller-service-uuid>"
    return str(value)


def _validation_is_valid(entity):
    status = str(_validation_status(entity) or "").upper()
    return status == "VALID"


def _await_validation(candidate, timeout=30, interval=1):
    deadline = time.time() + timeout
    while True:
        entity = _fetch_target_entity(candidate)
        status = str(_validation_status(entity) or "").upper()
        if status == "VALID":
            return entity
        if status == "INVALID":
            return entity
        if time.time() >= deadline:
            raise RuntimeError(
                f"{_controller_service_target_summary(candidate)} did not reach VALID within {timeout}s "
                f"(last status: {status or 'PENDING'})"
            )
        time.sleep(interval)


def _is_processor_running(entity):
    return str(_processor_run_status(entity) or "").upper() == "RUNNING"


def _is_controller_service_enabled(entity):
    return str(_component_state(entity) or "").upper() == "ENABLED"


def _target_operational_state(candidate, entity):
    if candidate["kind"] == "processor":
        return str(_processor_run_status(entity) or "").upper() or "STOPPED"
    return str(_component_state(entity) or "").upper() or "DISABLED"


def _flow_is_running(pg):
    running_count = int(_get(pg, "running_count", "runningCount", default=0) or 0)
    stopped_count = int(_get(pg, "stopped_count", "stoppedCount", default=0) or 0)
    return running_count > 0 and stopped_count == 0


def _build_process_group_path_resolver(flow_pg_id, flow_name):
    api = nipyapi.nifi.ProcessGroupsApi()
    cache = {flow_pg_id: flow_name}

    def resolve(group_id):
        if not group_id:
            return flow_name
        if group_id in cache:
            return cache[group_id]
        pg = api.get_process_group(id=group_id)
        parent_group_id = _get(_component(pg), "parent_group_id", "parentGroupId")
        name = _get(_component(pg), "name", default=group_id)
        if parent_group_id == flow_pg_id:
            path = f"{flow_name}/{name}"
        else:
            path = f"{resolve(parent_group_id)}/{name}"
        cache[group_id] = path
        return path

    return resolve


def discover_service_binding_targets(flow_pg_id, flow_name):
    processors_api = nipyapi.nifi.ProcessGroupsApi()
    flow_api = nipyapi.nifi.FlowApi()
    resolve_path = _build_process_group_path_resolver(flow_pg_id, flow_name)

    processors = processors_api.get_processors(id=flow_pg_id, include_descendant_groups=True)
    controller_services = flow_api.get_controller_services_from_group(
        id=flow_pg_id,
        include_ancestor_groups=False,
        include_descendant_groups=True,
    )

    candidates = []
    for processor in _get(processors, "processors", default=[]) or []:
        candidates.append({
            "id": _entity_id(processor),
            "name": _component_name(processor),
            "kind": "processor",
            "process_group_path": resolve_path(_component_parent_group_id(processor)),
        })
    for controller_service in _get(controller_services, "controller_services", default=[]) or []:
        candidates.append({
            "id": _entity_id(controller_service),
            "name": _component_name(controller_service),
            "kind": "controller_service",
            "process_group_path": resolve_path(_component_parent_group_id(controller_service)),
        })
    return candidates


def resolve_flow_process_group(flow_name):
    flow_name_upper = flow_name.upper()
    matches = [pg for pg in list_process_groups() if _component_name(pg).upper() == flow_name_upper]
    if not matches:
        raise RuntimeError(f"Flow process group '{flow_name}' was not found")
    if len(matches) > 1:
        available = ", ".join(sorted(_component_name(pg) for pg in matches))
        raise RuntimeError(
            f"Flow process group '{flow_name}' is ambiguous across root process groups: {available}. "
            f"Rename the flow process groups to unique names before using service_bindings."
        )
    return matches[0]


def resolve_service_binding_target(target_name, candidates):
    matches = [candidate for candidate in candidates if candidate["name"] == target_name]
    if not matches:
        raise RuntimeError(f"Service binding target '{target_name}' was not found in the imported flow")
    if len(matches) > 1:
        descriptions = ", ".join(
            f"{candidate['kind']} at {candidate['process_group_path']}"
            for candidate in matches
        )
        raise RuntimeError(f"Service binding target '{target_name}' is ambiguous: {descriptions}")
    return matches[0]


def build_root_pg_service_maps(declared_root_pg_services):
    declared_by_name = defaultdict(list)
    for service in declared_root_pg_services or []:
        declared_by_name[service["name"]].append(service)
    duplicate_declared = [name for name, values in declared_by_name.items() if len(values) > 1]
    if duplicate_declared:
        duplicates = ", ".join(sorted(duplicate_declared))
        raise RuntimeError(f"Duplicate root_pg_controller_services declarations are not supported: {duplicates}")

    live_services = list_root_pg_controller_services()
    live_by_name = defaultdict(list)
    for service in live_services:
        name = _component_name(service)
        live_by_name[name].append(service)

    name_to_id = {}
    for name in declared_by_name:
        matches = live_by_name.get(name, [])
        if len(matches) != 1:
            if not matches:
                raise RuntimeError(f"Referenced root PG controller service '{name}' was not found on the runtime")
            raise RuntimeError(f"Referenced root PG controller service '{name}' is ambiguous on the runtime")
        name_to_id[name] = _entity_id(matches[0])
    return name_to_id


def build_root_pg_service_uuid_to_name_map(live_services):
    reverse_map = {}
    grouped = defaultdict(list)
    for service in live_services or []:
        grouped[_get(service, "name")].append(service)
    for name, services in grouped.items():
        if not name or len(services) != 1:
            continue
        service = services[0]
        service_id = _get(service, "id") or _entity_id(service)
        if service_id:
            reverse_map[str(service_id)] = name
    return reverse_map


def _fetch_target_entity(candidate):
    if candidate["kind"] == "processor":
        return nipyapi.nifi.ProcessorsApi().get_processor(id=candidate["id"])
    return nipyapi.nifi.ControllerServicesApi().get_controller_service(id=candidate["id"])


def _properties_for_candidate(entity, candidate):
    if candidate["kind"] == "processor":
        return _processor_properties(entity)
    return _controller_service_properties(entity)


def _descriptors_for_candidate(entity, candidate):
    if candidate["kind"] == "processor":
        return _processor_descriptors(entity)
    return _controller_service_descriptors(entity)


def _ensure_existing_descriptor_is_supported(candidate, property_name, descriptor, removal_attempt):
    if not _descriptor_identifies_controller_service(descriptor):
        raise RuntimeError(
            f"{_controller_service_target_summary(candidate)} property '{property_name}' does not identify a controller service"
        )
    if _descriptor_sensitive(descriptor):
        raise RuntimeError(
            f"{_controller_service_target_summary(candidate)} property '{property_name}' is sensitive and cannot be managed via service_bindings"
        )
    if removal_attempt and _descriptor_required(descriptor):
        raise RuntimeError(
            f"{_controller_service_target_summary(candidate)} property '{property_name}' is required and cannot be removed"
        )


def inspect_service_binding_entry(candidate, binding_spec, root_pg_service_name_to_id):
    entity = _fetch_target_entity(candidate)
    properties = _properties_for_candidate(entity, candidate)
    descriptors = _descriptors_for_candidate(entity, candidate)

    resolved_properties = {}
    probe_properties = set()
    drift = False

    for property_name, desired_service_name in binding_spec.get("properties", {}).items():
        descriptor = descriptors.get(property_name)
        if desired_service_name is None:
            if descriptor is not None:
                _ensure_existing_descriptor_is_supported(candidate, property_name, descriptor, removal_attempt=True)
            if property_name in properties:
                drift = True
            continue

        if desired_service_name not in root_pg_service_name_to_id:
            raise RuntimeError(
                f"Service binding for {_controller_service_target_summary(candidate)} references unknown root PG controller service '{desired_service_name}'"
            )
        resolved_properties[property_name] = root_pg_service_name_to_id[desired_service_name]
        if descriptor is None:
            probe_properties.add(property_name)
        else:
            _ensure_existing_descriptor_is_supported(candidate, property_name, descriptor, removal_attempt=False)

        current_value = properties.get(property_name, _ABSENT)
        if current_value != resolved_properties[property_name]:
            drift = True

    return {
        "candidate": candidate,
        "binding": binding_spec,
        "resolved_properties": resolved_properties,
        "probe_properties": probe_properties,
        "needs_change": drift,
    }


def _capture_binding_restore_context(plan):
    candidate = plan["candidate"]
    binding_spec = plan["binding"]
    entity = _fetch_target_entity(candidate)
    properties = _properties_for_candidate(entity, candidate)
    return {
        "candidate": candidate,
        "binding": binding_spec,
        "original_entity": entity,
        "original_values": {
            property_name: properties.get(property_name, _ABSENT)
            for property_name in binding_spec.get("properties", {})
        },
        "original_target_state": _target_operational_state(candidate, entity),
    }


def _stop_processor_if_needed(candidate, entity):
    if not _is_processor_running(entity):
        return entity
    body = _build_run_status_entity(entity, "STOPPED", ["ProcessorRunStatusEntity"])
    method, _ = _select_api_method(nipyapi.nifi.ProcessorsApi(), ["update_run_status4"], "ProcessorsApi")
    method(id=candidate["id"], body=body)
    deadline = time.time() + 30
    while True:
        entity = _fetch_target_entity(candidate)
        if not _is_processor_running(entity):
            return entity
        if time.time() >= deadline:
            raise RuntimeError(f"{_controller_service_target_summary(candidate)} did not stop within 30s")
        time.sleep(1)


def _disable_controller_service_if_needed(candidate, entity):
    if not _is_controller_service_enabled(entity):
        return entity
    body = _build_model(
        "ControllerServiceRunStatusEntity",
        revision=_get(entity, "revision"),
        state="DISABLED",
    )
    method, _ = _select_api_method(
        nipyapi.nifi.ControllerServicesApi(),
        ["update_run_status1", "update_run_status2"],
        "ControllerServicesApi",
    )
    method(id=candidate["id"], body=body)
    deadline = time.time() + 30
    while True:
        entity = _fetch_target_entity(candidate)
        if not _is_controller_service_enabled(entity):
            return entity
        if time.time() >= deadline:
            raise RuntimeError(f"{_controller_service_target_summary(candidate)} did not disable within 30s")
        time.sleep(1)


def _prepare_target_for_update(candidate, entity):
    if candidate["kind"] == "processor":
        return _stop_processor_if_needed(candidate, entity)
    return _disable_controller_service_if_needed(candidate, entity)


def _update_target_properties(candidate, entity, properties):
    revision = _get(entity, "revision")
    if candidate["kind"] == "processor":
        body = _build_model(
            "ProcessorEntity",
            id=candidate["id"],
            revision=revision,
            component=_build_model(
                "ProcessorDTO",
                id=candidate["id"],
                config=_build_model("ProcessorConfigDTO", properties=properties),
            ),
        )
        return nipyapi.nifi.ProcessorsApi().update_processor(id=candidate["id"], body=body)

    body = _build_model(
        "ControllerServiceEntity",
        id=candidate["id"],
        revision=revision,
        component=_build_model(
            "ControllerServiceDTO",
            id=candidate["id"],
            properties=properties,
        ),
    )
    return nipyapi.nifi.ControllerServicesApi().update_controller_service(id=candidate["id"], body=body)


def _verify_probe_results(candidate, entity, probe_properties):
    if not probe_properties:
        return
    descriptors = _descriptors_for_candidate(entity, candidate)
    for property_name in probe_properties:
        descriptor = descriptors.get(property_name)
        if descriptor is None:
            raise RuntimeError(
                f"{_controller_service_target_summary(candidate)} property '{property_name}' did not materialize after the dynamic-property probe"
            )
        if not _descriptor_dynamic(descriptor):
            raise RuntimeError(
                f"{_controller_service_target_summary(candidate)} property '{property_name}' did not materialize as a dynamic property"
            )
        _ensure_existing_descriptor_is_supported(candidate, property_name, descriptor, removal_attempt=False)


def _verify_applied_properties(candidate, entity, binding_spec, resolved_properties):
    properties = _properties_for_candidate(entity, candidate)
    for property_name, desired_service_name in binding_spec.get("properties", {}).items():
        if desired_service_name is None:
            if property_name in properties:
                raise RuntimeError(
                    f"{_controller_service_target_summary(candidate)} property '{property_name}' could not be removed"
                )
            continue
        desired_id = resolved_properties[property_name]
        actual_value = properties.get(property_name)
        if actual_value != desired_id:
            raise RuntimeError(
                f"{_controller_service_target_summary(candidate)} property '{property_name}' was expected to reference '{desired_service_name}' but was '{_property_value_repr(actual_value)}'"
            )


def _set_target_operational_state(candidate, state):
    state = str(state or "").upper()
    if candidate["kind"] == "processor":
        current = _fetch_target_entity(candidate)
        if _target_operational_state(candidate, current) == state:
            return current
        body = _build_run_status_entity(current, state, ["ProcessorRunStatusEntity"])
        method, _ = _select_api_method(nipyapi.nifi.ProcessorsApi(), ["update_run_status4"], "ProcessorsApi")
        method(id=candidate["id"], body=body)
        deadline = time.time() + 30
        while True:
            current = _fetch_target_entity(candidate)
            if _target_operational_state(candidate, current) == state:
                return current
            if time.time() >= deadline:
                raise RuntimeError(f"{_controller_service_target_summary(candidate)} did not reach state {state} within 30s")
            time.sleep(1)

    current = _fetch_target_entity(candidate)
    if _target_operational_state(candidate, current) == state:
        return current
    body = _build_model(
        "ControllerServiceRunStatusEntity",
        revision=_get(current, "revision"),
        state=state,
    )
    method, _ = _select_api_method(
        nipyapi.nifi.ControllerServicesApi(),
        ["update_run_status1", "update_run_status2"],
        "ControllerServicesApi",
    )
    method(id=candidate["id"], body=body)
    deadline = time.time() + 30
    while True:
        current = _fetch_target_entity(candidate)
        if _target_operational_state(candidate, current) == state:
            return current
        if time.time() >= deadline:
            raise RuntimeError(f"{_controller_service_target_summary(candidate)} did not reach state {state} within 30s")
        time.sleep(1)


def _rollback_binding_entry(candidate, original_values, original_target_state=None):
    entity = _prepare_target_for_update(candidate, _fetch_target_entity(candidate))
    rollback_properties = {
        property_name: (None if value is _ABSENT else value)
        for property_name, value in original_values.items()
    }
    _update_target_properties(candidate, entity, rollback_properties)
    restored_entity = _fetch_target_entity(candidate)
    if original_target_state:
        restored_entity = _set_target_operational_state(candidate, original_target_state)
    return restored_entity


def apply_service_binding_entry(plan, restore_context):
    candidate = plan["candidate"]
    binding_spec = plan["binding"]
    entity = _prepare_target_for_update(candidate, restore_context["original_entity"])
    _update_target_properties(candidate, entity, {
        property_name: plan["resolved_properties"].get(property_name)
        if desired_service_name is not None else None
        for property_name, desired_service_name in binding_spec.get("properties", {}).items()
    })
    entity = _fetch_target_entity(candidate)
    _verify_probe_results(candidate, entity, plan["probe_properties"])
    entity = _await_validation(candidate)
    if not _validation_is_valid(entity):
        errors = "; ".join(str(error) for error in _validation_errors(entity)) or (_validation_status(entity) or "unknown validation error")
        raise RuntimeError(f"{_controller_service_target_summary(candidate)} did not validate after binding update: {errors}")
    _verify_applied_properties(candidate, entity, binding_spec, plan["resolved_properties"])
    return entity


def _sanitize_exception(exc):
    status = _get(exc, "status")
    reason = _get(exc, "reason")
    if status is not None or reason:
        if status is not None and reason:
            return f"HTTP {status} {reason}"
        if status is not None:
            return f"HTTP {status}"
        return str(reason)
    return str(exc)


def _restore_binding_context(restore_context):
    return _rollback_binding_entry(
        restore_context["candidate"],
        restore_context["original_values"],
        restore_context.get("original_target_state"),
    )


def describe_flow_service_bindings(flow_pg_id, flow_name, binding_specs, root_pg_service_uuid_to_name):
    candidates = discover_service_binding_targets(flow_pg_id, flow_name)
    described = []
    for binding_spec in binding_specs or []:
        entry = {
            "target": binding_spec["target"],
            "state": "known",
            "properties": {},
            "issues": [],
        }
        try:
            candidate = resolve_service_binding_target(binding_spec["target"], candidates)
            entity = _fetch_target_entity(candidate)
            descriptors = _descriptors_for_candidate(entity, candidate)
            properties = _properties_for_candidate(entity, candidate)
            entry["target_kind"] = candidate["kind"]
            entry["process_group_path"] = candidate["process_group_path"]
            entry["validation_status"] = _validation_status(entity)
            entry["validation_errors"] = _validation_errors(entity)
            for property_name in binding_spec.get("properties", {}):
                descriptor = descriptors.get(property_name)
                live_value = properties.get(property_name)
                property_state = {"configured": property_name in properties}
                if live_value is None:
                    property_state["service"] = None
                elif str(live_value) in root_pg_service_uuid_to_name:
                    property_state["service"] = root_pg_service_uuid_to_name[str(live_value)]
                elif _UUID_RE.match(str(live_value or "")):
                    property_state["service"] = None
                    property_state["warning"] = "Unmanaged controller service reference"
                else:
                    property_state["service"] = None
                    property_state["warning"] = "Non-controller-service value configured"
                if descriptor is not None:
                    property_state["dynamic"] = _descriptor_dynamic(descriptor)
                    property_state["required"] = _descriptor_required(descriptor)
                    if not _descriptor_identifies_controller_service(descriptor):
                        property_state["warning"] = "Property is not a controller service reference"
                    if _descriptor_sensitive(descriptor):
                        property_state["warning"] = "Sensitive controller service bindings are unsupported"
                entry["properties"][property_name] = property_state
        except Exception as exc:
            entry["state"] = "unknown"
            entry["issues"].append(_sanitize_exception(exc))
        described.append(entry)
    return described


def reconcile_service_bindings(flow_spec, root_pg_controller_service_specs, runtime_url, nifi_pat, nifi_auth=None):
    binding_specs = flow_spec.get("service_bindings") or []
    if not binding_specs:
        return False
    if "start" not in flow_spec:
        raise RuntimeError(f"Flow '{flow_spec['name']}' declares service_bindings and must explicitly set start")

    configure_nifi(runtime_url, pat=nifi_pat, nifi_auth=nifi_auth)
    pg = resolve_flow_process_group(flow_spec["name"])

    root_name_to_id = build_root_pg_service_maps(root_pg_controller_service_specs)
    candidates = discover_service_binding_targets(pg.id, flow_spec["name"])

    plans = []
    any_drift = False
    for binding_spec in binding_specs:
        candidate = resolve_service_binding_target(binding_spec["target"], candidates)
        plan = inspect_service_binding_entry(candidate, binding_spec, root_name_to_id)
        plans.append(plan)
        any_drift = any_drift or plan["needs_change"]

    if not any_drift:
        print(f"[bindings] '{flow_spec['name']}' already matches the declared service bindings")
        return False

    flow_was_running = _flow_is_running(pg)
    _quiesce_flow(pg.id, flow_spec["name"])
    applied_contexts = []
    current_context = None
    try:
        for plan in plans:
            if not plan["needs_change"]:
                continue
            current_context = _capture_binding_restore_context(plan)
            apply_service_binding_entry(plan, current_context)
            applied_contexts.append(current_context)
            current_context = None
    except Exception as original_error:
        restoration_failures = []
        if current_context is not None:
            try:
                _restore_binding_context(current_context)
            except Exception as exc:
                restoration_failures.append(f"current entry restore failed: {_sanitize_exception(exc)}")
        for restore_context in reversed(applied_contexts):
            try:
                _restore_binding_context(restore_context)
            except Exception as exc:
                restoration_failures.append(
                    f"restore failed for {_controller_service_target_summary(restore_context['candidate'])}: {_sanitize_exception(exc)}"
                )
        if flow_was_running:
            try:
                start_flow(pg.id, flow_spec["name"])
            except Exception as exc:
                restoration_failures.append(f"flow restoration failed: {_sanitize_exception(exc)}")
        if restoration_failures:
            raise RuntimeError(
                f"Service binding sequence failed for flow '{flow_spec['name']}': {_sanitize_exception(original_error)}. "
                f"Restoration failures: {'; '.join(restoration_failures)}"
            ) from original_error
        raise RuntimeError(
            f"Service binding sequence failed for flow '{flow_spec['name']}': {_sanitize_exception(original_error)}. "
            f"The original values and run state were restored."
        ) from original_error
    return True