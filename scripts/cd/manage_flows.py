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
import time

import nipyapi

from safe_exceptions import format_safe_exception


DELETE_POLL_INTERVAL_SECONDS = 1
DELETE_STATE_TIMEOUT_SECONDS = 60
DROP_REQUEST_TIMEOUT_SECONDS = 60


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


def _revision_version(entity):
    revision = _get(entity, "revision")
    return _get(revision, "version")


def _processor_state(entity):
    status = _get(entity, "status")
    return str(
        _get(status, "run_status", "runStatus")
        or _get(_component(entity), "state")
        or ""
    ).upper()


def _processor_active_thread_count(entity):
    status = _get(entity, "status")
    snapshot = _get(status, "aggregate_snapshot", "aggregateSnapshot")
    return _get(snapshot, "active_thread_count", "activeThreadCount")


def _controller_service_state(entity):
    return str(_get(_component(entity), "state") or "").upper()


def _processor_is_deletable(entity):
    return (
        _processor_state(entity) in {"STOPPED", "DISABLED", "INVALID"}
        and _processor_active_thread_count(entity) == 0
    )


def _processor_delete_state(entity):
    state = _processor_state(entity) or "UNKNOWN"
    active_threads = _processor_active_thread_count(entity)
    thread_summary = "UNKNOWN" if active_threads is None else str(active_threads)
    return f"{state}, active threads={thread_summary}"


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


def _format_component_state(entity, state, resolve_path):
    path = resolve_path(_component_parent_group_id(entity))
    component_id = _entity_id(entity)
    component_name = _component_name(entity) or component_id or "<unnamed>"
    return f"{path}/{component_name} [{component_id}]={state or 'UNKNOWN'}"


def _summarize_components(entities, state_getter, resolve_path):
    return ", ".join(
        _format_component_state(entity, state_getter(entity), resolve_path)
        for entity in entities
    )


def _select_api_method(api, candidate_names, api_name):
    for candidate_name in candidate_names:
        method = getattr(api, candidate_name, None)
        if callable(method):
            return method, candidate_name
    names = ", ".join(candidate_names)
    raise AttributeError(f"{api_name} does not provide a supported method ({names})")


def _is_unsupported_kwarg_type_error(exc, kwarg_names):
    message = str(exc)
    if "unexpected keyword argument" not in message:
        return False
    return any(f"'{kwarg_name}'" in message for kwarg_name in kwarg_names)


def _call_api_with_legacy_kwarg_fallback(api_method, base_kwargs, optional_kwargs):
    kwargs = {**base_kwargs, **optional_kwargs}
    try:
        return api_method(**kwargs)
    except TypeError as exc:
        if not optional_kwargs or not _is_unsupported_kwarg_type_error(exc, optional_kwargs):
            raise
    return api_method(**base_kwargs)


def _list_descendant_processors(pg_id):
    result = _call_api_with_legacy_kwarg_fallback(
        nipyapi.nifi.ProcessGroupsApi().get_processors,
        {"id": pg_id},
        {"include_descendant_groups": True},
    )
    return list(_get(result, "processors", default=[]) or [])


def _list_descendant_controller_services(pg_id):
    result = _call_api_with_legacy_kwarg_fallback(
        nipyapi.nifi.FlowApi().get_controller_services_from_group,
        {"id": pg_id},
        {
            "include_ancestor_groups": False,
            "include_descendant_groups": True,
        },
    )
    return list(_get(result, "controller_services", default=[]) or [])


def _await_deletable_flow_state(
    pg_id,
    flow_name,
    timeout=DELETE_STATE_TIMEOUT_SECONDS,
    interval=DELETE_POLL_INTERVAL_SECONDS,
):
    resolve_path = _build_process_group_path_resolver(pg_id, flow_name)
    deadline = time.time() + timeout
    while True:
        try:
            processors = _list_descendant_processors(pg_id)
            controller_services = _list_descendant_controller_services(pg_id)
        except Exception as exc:
            raise RuntimeError(
                f"Failed to inspect deletable state for flow '{flow_name}': {format_safe_exception(exc)}"
            ) from None

        active_processors = [processor for processor in processors if not _processor_is_deletable(processor)]
        active_controller_services = [
            controller_service
            for controller_service in controller_services
            if _controller_service_state(controller_service) != "DISABLED"
        ]

        if not active_processors and not active_controller_services:
            return

        if time.time() >= deadline:
            details = []
            if active_processors:
                details.append(
                    "processors not in a deletable state: "
                    + _summarize_components(active_processors, _processor_delete_state, resolve_path)
                )
            if active_controller_services:
                details.append(
                    "controller services not DISABLED: "
                    + _summarize_components(active_controller_services, _controller_service_state, resolve_path)
                )
            raise RuntimeError(
                f"Flow '{flow_name}' did not reach a deletable state within {timeout}s: {'; '.join(details)}"
            )

        time.sleep(interval)


def _prepare_flow_for_delete(pg_id, flow_name):
    flow_api = nipyapi.nifi.FlowApi()
    try:
        flow_api.schedule_components(
            id=pg_id,
            body=nipyapi.nifi.ScheduleComponentsEntity(id=pg_id, state="STOPPED"),
        )
    except Exception as exc:
        raise RuntimeError(
            f"Failed to stop processors for flow '{flow_name}': {format_safe_exception(exc)}"
        ) from None

    try:
        flow_api.activate_controller_services(
            id=pg_id,
            body=nipyapi.nifi.ActivateControllerServicesEntity(id=pg_id, state="DISABLED"),
        )
    except Exception as exc:
        raise RuntimeError(
            f"Failed to disable controller services for flow '{flow_name}': {format_safe_exception(exc)}"
        ) from None

    _await_deletable_flow_state(pg_id, flow_name)


def _drop_all_flowfiles(
    pg_id,
    flow_name,
    timeout=DROP_REQUEST_TIMEOUT_SECONDS,
    interval=DELETE_POLL_INTERVAL_SECONDS,
):
    pg_api = nipyapi.nifi.ProcessGroupsApi()
    try:
        drop_response = pg_api.create_empty_all_connections_request(id=pg_id)
    except Exception as exc:
        raise RuntimeError(
            f"Failed to drop queued flowfiles for flow '{flow_name}': {format_safe_exception(exc)}"
        ) from None

    drop_request = _get(drop_response, "drop_request", "dropRequest")
    drop_request_id = _get(drop_request, "id")
    if not drop_request_id:
        raise RuntimeError(f"Failed to drop queued flowfiles for flow '{flow_name}': missing drop request id")

    operation_error = None
    try:
        deadline = time.time() + timeout
        while True:
            status_response = pg_api.get_drop_all_flowfiles_request(id=pg_id, drop_request_id=drop_request_id)
            status = _get(status_response, "drop_request", "dropRequest")
            failure_reason = _get(status, "failure_reason", "failureReason")
            state = str(_get(status, "state") or "").upper()
            finished = bool(_get(status, "finished", default=False))
            if failure_reason or state == "FAILURE":
                raise RuntimeError(
                    f"Queue drop request for flow '{flow_name}' failed: {failure_reason or 'drop request reported FAILURE'}"
                )
            if finished:
                break
            if time.time() >= deadline:
                raise RuntimeError(
                    f"Queue drop request for flow '{flow_name}' did not finish within {timeout}s"
                )
            time.sleep(interval)
    except Exception as exc:
        operation_error = exc

    cleanup_error = None
    try:
        remove_drop_request, _ = _select_api_method(
            pg_api,
            ["remove_drop_request1", "remove_drop_request"],
            "ProcessGroupsApi",
        )
        remove_drop_request(id=pg_id, drop_request_id=drop_request_id)
    except Exception as exc:
        cleanup_error = exc

    if operation_error is not None:
        message = str(operation_error)
        if cleanup_error is not None:
            message = (
                f"{message}. Failed to remove queue drop request for flow '{flow_name}': "
                f"{format_safe_exception(cleanup_error)}"
            )
        raise RuntimeError(message) from None

    if cleanup_error is not None:
        raise RuntimeError(
            f"Failed to remove queue drop request for flow '{flow_name}': {format_safe_exception(cleanup_error)}"
        ) from None


def _get_process_group_for_delete(pg_id, flow_name):
    try:
        return nipyapi.nifi.ProcessGroupsApi().get_process_group(id=pg_id)
    except Exception as exc:
        raise RuntimeError(
            f"Failed to refresh process group '{flow_name}' before delete: {format_safe_exception(exc)}"
        ) from None


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


def _remove_process_group(pg_id, flow_name):
    pg_api = nipyapi.nifi.ProcessGroupsApi()
    for attempt in range(2):
        if attempt:
            _await_deletable_flow_state(pg_id, flow_name)
        pg_entity = _get_process_group_for_delete(pg_id, flow_name)
        try:
            pg_api.remove_process_group(id=pg_id, version=str(_revision_version(pg_entity)))
            return
        except Exception as exc:
            if attempt == 0 and _is_revision_conflict(exc):
                continue
            action = "retry delete" if attempt else "delete"
            raise RuntimeError(
                f"Failed to {action} process group '{flow_name}': {format_safe_exception(exc)}"
            ) from None


def configure_nifi(runtime_url, pat=None, nifi_auth=None):
    """Configure nipyapi to connect to a NiFi instance.

    Args:
        runtime_url: NiFi base URL (with or without /nifi-api suffix).
        pat: Bearer token (Snowflake PAT). Used when nifi_auth is not provided.
        nifi_auth: Dict with auth config. Supported types:
            {"type": "username_password", "username": "...", "password": "...",
             "verify_ssl": True}
            When provided, uses nipyapi.security.service_login() to obtain a JWT
            via POST /nifi-api/access/token (standard OSS NiFi auth flow).
    """
    api_url = runtime_url.rstrip("/")
    if api_url.endswith("/nifi"):
        api_url = api_url[:-5]
    if not api_url.endswith("/nifi-api"):
        api_url += "/nifi-api"
    nipyapi.config.nifi_config.host = api_url
    nipyapi.config.nifi_config.api_client = None

    if nifi_auth and nifi_auth.get("type") == "username_password":
        verify_ssl = nifi_auth.get("verify_ssl", True)
        nipyapi.config.nifi_config.verify_ssl = verify_ssl
        nipyapi.security.service_login(
            service="nifi",
            username=nifi_auth["username"],
            password=nifi_auth["password"],
        )
    elif pat:
        nipyapi.config.nifi_config.api_key["bearerAuth"] = f"Bearer {pat}"


def find_registry_client(name):
    api = nipyapi.nifi.ControllerApi()
    result = api.get_flow_registry_clients()
    for rc in (result.registries or []):
        if rc.component.name == name:
            return rc
    return None


def get_root_pg_id():
    return nipyapi.canvas.get_root_pg_id()


def resolve_version(registry_client_id, bucket_id, flow_id, version_spec):
    """Resolve a version spec to the actual version string.

    If version_spec is 'latest', fetches all versions from the registry and
    returns the version string of the most recent snapshot (by timestamp).
    Otherwise returns version_spec unchanged.
    """
    if version_spec != "latest":
        return version_spec

    api = nipyapi.nifi.FlowApi()
    result = api.get_versions(
        registry_id=registry_client_id,
        bucket_id=bucket_id,
        flow_id=flow_id,
    )
    snapshots = result.versioned_flow_snapshot_metadata_set or []
    if not snapshots:
        raise RuntimeError(
            f"No versions found for flow '{flow_id}' in bucket '{bucket_id}'"
        )
    latest = max(
        snapshots,
        key=lambda s: s.versioned_flow_snapshot_metadata.timestamp,
    )
    version = latest.versioned_flow_snapshot_metadata.version
    print(f"[flow] Resolved 'latest' -> '{version}' for {bucket_id}/{flow_id}")
    return version


def list_process_groups(parent_id=None):
    if parent_id is None:
        parent_id = get_root_pg_id()
    api = nipyapi.nifi.ProcessGroupsApi()
    result = api.get_process_groups(parent_id)
    return result.process_groups or []


def find_flow_pg_by_name(pg_name, parent_id=None):
    for pg in list_process_groups(parent_id):
        if pg.component.name == pg_name:
            return pg
    return None


def import_flow(registry_client_id, bucket, flow_name, version, pg_name, parent_id=None, dedicated_parameter_context=False):
    if parent_id is None:
        parent_id = get_root_pg_id()

    position = nipyapi.layout.suggest_pg_position(parent_id)

    api = nipyapi.nifi.ProcessGroupsApi()

    body = nipyapi.nifi.ProcessGroupEntity(
        revision=nipyapi.nifi.RevisionDTO(version=0),
        component=nipyapi.nifi.ProcessGroupDTO(
            name=pg_name,
            position=nipyapi.nifi.PositionDTO(x=position[0], y=position[1]),
            version_control_information=nipyapi.nifi.VersionControlInformationDTO(
                registry_id=registry_client_id,
                bucket_id=bucket,
                bucket_name=bucket,
                flow_id=flow_name,
                flow_name=flow_name,
                version=version,
            ),
        ),
    )

    strategy = "REPLACE" if dedicated_parameter_context else "KEEP_EXISTING"
    print(f"[flow] Importing {bucket}/{flow_name} '{version}' as '{pg_name}' (param context: {strategy})...")
    result = api.create_process_group(id=parent_id, body=body, parameter_context_handling_strategy=strategy)

    if result.component.name != pg_name:
        rename_body = nipyapi.nifi.ProcessGroupEntity(
            id=result.id,
            revision=result.revision,
            component=nipyapi.nifi.ProcessGroupDTO(
                id=result.id,
                name=pg_name,
            ),
        )
        result = api.update_process_group(id=result.id, body=rename_body)
        print(f"[flow] Renamed PG to '{pg_name}'")

    print(f"[flow] Imported '{pg_name}' (pg_id={result.id})")
    return result


def update_flow_version(pg_entity, new_version):
    vci = pg_entity.component.version_control_information
    print(f"[flow] Updating {vci.bucket_name}/{vci.flow_name} to '{new_version}'...")
    print(f"[flow] Current VCI state: '{vci.state}' — reverting local changes first...")

    try:
        nipyapi.versioning.revert_flow_ver(pg_entity, wait=True)
        print(f"[flow] Reverted local modifications")
    except Exception as e:
        print(f"[flow] Revert skipped or failed: {e}")

    import time
    time.sleep(2)
    api = nipyapi.nifi.ProcessGroupsApi()
    pg_entity = api.get_process_group(id=pg_entity.id)
    result = nipyapi.versioning.update_git_flow_ver(pg_entity, target_version=str(new_version))
    print(f"[flow] Updated to '{new_version}'")
    return result


def delete_flow(pg_entity):
    pg_id = pg_entity.id
    name = pg_entity.component.name
    print(f"[flow] Deleting process group '{name}' ({pg_id})...")

    _prepare_flow_for_delete(pg_id, name)
    _drop_all_flowfiles(pg_id, name)
    _remove_process_group(pg_id, name)
    print(f"[flow] Deleted process group '{name}'")


def reconcile_flows(flows, registry_client_name, runtime_url, nifi_pat, nifi_auth=None):
    """Idempotent reconcile: create missing PGs, update version-mismatched PGs, skip up-to-date ones."""
    configure_nifi(runtime_url, pat=nifi_pat, nifi_auth=nifi_auth)

    rc = find_registry_client(registry_client_name)
    if not rc:
        raise RuntimeError(f"Flow Registry Client '{registry_client_name}' not found")

    for flow_spec in flows:
        pg_name = flow_spec["name"]
        bucket = flow_spec["bucket"]
        flow_name = flow_spec["flow"]
        version_spec = flow_spec["version"]

        desired_version = resolve_version(rc.id, bucket, flow_name, version_spec)

        pg = find_flow_pg_by_name(pg_name)
        if not pg:
            import_flow(rc.id, bucket, flow_name, desired_version, pg_name,
                        dedicated_parameter_context=flow_spec.get("dedicated_parameter_context", False))
        else:
            vci = pg.component.version_control_information
            current_version = vci.version if vci else None
            if current_version != desired_version:
                print(f"[flow] '{pg_name}' is at '{current_version}', updating to '{desired_version}'...")
                update_flow_version(pg, desired_version)
            else:
                print(f"[flow] '{pg_name}' already at '{desired_version}' -- no change")


def start_flow(pg_id, pg_name=""):
    """Enable controller services in a PG then start all processors."""
    label = pg_name or pg_id
    print(f"[flow] Enabling controller services in '{label}'...")
    body = nipyapi.nifi.ActivateControllerServicesEntity(
        id=pg_id,
        state="ENABLED",
    )
    nipyapi.nifi.FlowApi().activate_controller_services(id=pg_id, body=body)
    time.sleep(3)
    print(f"[flow] Starting processors in '{label}'...")
    nipyapi.canvas.schedule_process_group(pg_id, True)
    print(f"[flow] '{label}' started")


def stop_flow(pg_id, pg_name=""):
    label = pg_name or pg_id
    print(f"[flow] Stopping processors in '{label}'...")
    nipyapi.canvas.schedule_process_group(pg_id, False)
    time.sleep(5)
    print(f"[flow] Disabling controller services in '{label}'...")
    body = nipyapi.nifi.ActivateControllerServicesEntity(
        id=pg_id,
        state="DISABLED",
    )
    nipyapi.nifi.FlowApi().activate_controller_services(id=pg_id, body=body)
    time.sleep(3)
    print(f"[flow] '{label}' stopped")


def delete_flows(flows, registry_client_name, runtime_url, nifi_pat, nifi_auth=None):
    """Delete process groups for flows that were removed from config."""
    try:
        configure_nifi(runtime_url, pat=nifi_pat, nifi_auth=nifi_auth)
    except Exception as exc:
        raise RuntimeError(
            f"Failed to configure NiFi client for flow delete: {format_safe_exception(exc)}"
        ) from None

    for flow_spec in flows:
        pg_name = flow_spec["name"]
        try:
            pg = find_flow_pg_by_name(pg_name)
        except Exception as exc:
            raise RuntimeError(
                f"Failed to locate process group '{pg_name}' for delete: {format_safe_exception(exc)}"
            ) from None
        if pg:
            delete_flow(pg)
        else:
            print(f"[flow] '{pg_name}' not found, skipping delete")


def main():
    parser = argparse.ArgumentParser(description="Manage flows on NiFi runtime")
    parser.add_argument("action", choices=["reconcile", "delete"])
    parser.add_argument("--flows", required=True, help="JSON array of flow specs")
    parser.add_argument("--registry-client-name", default="nifihub")
    parser.add_argument("--runtime-url", required=True)
    parser.add_argument("--nifi-pat", required=True)
    args = parser.parse_args()

    flows = json.loads(args.flows)
    if args.action == "reconcile":
        reconcile_flows(flows, args.registry_client_name, args.runtime_url, args.nifi_pat)
    elif args.action == "delete":
        delete_flows(flows, args.registry_client_name, args.runtime_url, args.nifi_pat)


if __name__ == "__main__":
    main()