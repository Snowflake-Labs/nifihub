#!/usr/bin/env python3
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

import types

import pytest


def _entity(
    component_id,
    name,
    parent_group_id,
    state,
    revision=0,
    processor=False,
    active_thread_count=0,
):
    component = types.SimpleNamespace(
        id=component_id,
        name=name,
        parent_group_id=parent_group_id,
        state=state if not processor else None,
    )
    status = (
        types.SimpleNamespace(
            run_status=state,
            aggregate_snapshot=types.SimpleNamespace(active_thread_count=active_thread_count),
        )
        if processor
        else None
    )
    return types.SimpleNamespace(
        id=component_id,
        component=component,
        status=status,
        revision=types.SimpleNamespace(version=revision),
    )


def _load_manage_flows(import_cd_module, fake_nipyapi):
    return import_cd_module("manage_flows", {"nipyapi": fake_nipyapi})


class _HeaderBodyOnlyError(Exception):
    def __str__(self):
        return (
            "HTTP response headers: Set-Cookie: secret-cookie\n"
            "Authorization: Bearer token\n"
            "HTTP response body: sensitive body"
        )


def _configure_delete_apis(fake_nipyapi, *, processors, controller_services, pg_revision=5):
    calls = []
    fake_nipyapi.api_methods["FlowApi"]["schedule_components"] = (
        lambda **kwargs: calls.append(("schedule", kwargs["id"], kwargs["body"].state))
    )
    fake_nipyapi.api_methods["FlowApi"]["activate_controller_services"] = (
        lambda **kwargs: calls.append(("activate", kwargs["id"], kwargs["body"].state))
    )
    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_processors"] = (
        lambda **kwargs: types.SimpleNamespace(processors=processors.pop(0) if len(processors) > 1 else processors[0])
    )
    fake_nipyapi.api_methods["FlowApi"]["get_controller_services_from_group"] = (
        lambda **kwargs: types.SimpleNamespace(
            controller_services=controller_services.pop(0) if len(controller_services) > 1 else controller_services[0]
        )
    )
    fake_nipyapi.api_methods["ProcessGroupsApi"]["create_empty_all_connections_request"] = (
        lambda **kwargs: types.SimpleNamespace(drop_request=types.SimpleNamespace(id="drop-1"))
    )
    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_drop_all_flowfiles_request"] = (
        lambda **kwargs: types.SimpleNamespace(
            drop_request=types.SimpleNamespace(id="drop-1", finished=True, failure_reason=None, state="SUCCESS")
        )
    )
    fake_nipyapi.api_methods["ProcessGroupsApi"]["remove_drop_request1"] = (
        lambda **kwargs: calls.append(("remove-drop", kwargs["drop_request_id"]))
    )
    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_process_group"] = (
        lambda **kwargs: _entity(kwargs["id"], "Flow", "root", None, revision=pg_revision)
    )
    fake_nipyapi.api_methods["ProcessGroupsApi"]["remove_process_group"] = (
        lambda **kwargs: calls.append(("remove-pg", kwargs["id"], kwargs["version"]))
    )
    return calls


def test_delete_flow_waits_for_processors_and_controller_services(import_cd_module, fake_nipyapi, monkeypatch):
    running = _entity("processor-1", "Processor", "flow-pg", "RUNNING", processor=True)
    stopped = _entity("processor-1", "Processor", "flow-pg", "STOPPED", processor=True)
    disabling = _entity("service-1", "Service", "flow-pg", "DISABLING")
    disabled = _entity("service-1", "Service", "flow-pg", "DISABLED")
    calls = _configure_delete_apis(
        fake_nipyapi,
        processors=[[running], [stopped]],
        controller_services=[[disabling], [disabled]],
    )
    module = _load_manage_flows(import_cd_module, fake_nipyapi)
    monkeypatch.setattr(module.time, "sleep", lambda _seconds: None)

    module.delete_flow(_entity("flow-pg", "Flow", "root", None, revision=1))

    assert calls == [
        ("schedule", "flow-pg", "STOPPED"),
        ("activate", "flow-pg", "DISABLED"),
        ("remove-drop", "drop-1"),
        ("remove-pg", "flow-pg", "5"),
    ]


def test_disable_timeout_blocks_queue_and_process_group_delete(import_cd_module, fake_nipyapi, monkeypatch):
    disabling = _entity("service-1", "Service", "flow-pg", "DISABLING")
    calls = _configure_delete_apis(
        fake_nipyapi,
        processors=[[]],
        controller_services=[[disabling]],
    )
    module = _load_manage_flows(import_cd_module, fake_nipyapi)
    monkeypatch.setattr(module.time, "sleep", lambda _seconds: None)

    with pytest.raises(RuntimeError, match="controller services not DISABLED"):
        module._await_deletable_flow_state("flow-pg", "Flow", timeout=0, interval=0)

    assert all(call[0] not in {"remove-drop", "remove-pg"} for call in calls)


@pytest.mark.parametrize("state", ["STOPPED", "DISABLED", "INVALID"])
def test_processor_terminal_states_are_deletable(import_cd_module, fake_nipyapi, state):
    processor = _entity("processor-1", "Processor", "flow-pg", state, processor=True)
    _configure_delete_apis(
        fake_nipyapi,
        processors=[[processor]],
        controller_services=[[]],
    )
    module = _load_manage_flows(import_cd_module, fake_nipyapi)

    module._await_deletable_flow_state("flow-pg", "Flow", timeout=0, interval=0)


@pytest.mark.parametrize("state", ["RUNNING", "VALIDATING", "UNKNOWN"])
def test_processor_nonterminal_and_unknown_states_block_delete(import_cd_module, fake_nipyapi, state):
    processor = _entity("processor-1", "Processor", "flow-pg", state, processor=True)
    _configure_delete_apis(
        fake_nipyapi,
        processors=[[processor]],
        controller_services=[[]],
    )
    module = _load_manage_flows(import_cd_module, fake_nipyapi)

    with pytest.raises(RuntimeError, match=f"processor-1.*={state}, active threads=0"):
        module._await_deletable_flow_state("flow-pg", "Flow", timeout=0, interval=0)


def test_stopped_processor_with_active_threads_blocks_delete(import_cd_module, fake_nipyapi):
    processor = _entity(
        "processor-1",
        "Processor",
        "flow-pg",
        "STOPPED",
        processor=True,
        active_thread_count=1,
    )
    _configure_delete_apis(fake_nipyapi, processors=[[processor]], controller_services=[[]])
    module = _load_manage_flows(import_cd_module, fake_nipyapi)

    with pytest.raises(RuntimeError, match="processor-1.*=STOPPED, active threads=1"):
        module._await_deletable_flow_state("flow-pg", "Flow", timeout=0, interval=0)


def test_processor_with_missing_thread_count_blocks_delete(import_cd_module, fake_nipyapi):
    processor = _entity("processor-1", "Processor", "flow-pg", "STOPPED", processor=True)
    processor.status.aggregate_snapshot.active_thread_count = None
    _configure_delete_apis(fake_nipyapi, processors=[[processor]], controller_services=[[]])
    module = _load_manage_flows(import_cd_module, fake_nipyapi)

    with pytest.raises(RuntimeError, match="processor-1.*=STOPPED, active threads=UNKNOWN"):
        module._await_deletable_flow_state("flow-pg", "Flow", timeout=0, interval=0)


def test_prepare_failure_is_sanitized_and_blocks_delete(import_cd_module, fake_nipyapi):
    class ApiError(Exception):
        status = 409
        reason = "Conflict"

        def __str__(self):
            return "HTTP response headers: Set-Cookie: secret\nHTTP response body: sensitive"

    fake_nipyapi.api_methods["FlowApi"]["schedule_components"] = lambda **kwargs: None
    fake_nipyapi.api_methods["FlowApi"]["activate_controller_services"] = (
        lambda **kwargs: (_ for _ in ()).throw(ApiError())
    )
    module = _load_manage_flows(import_cd_module, fake_nipyapi)

    with pytest.raises(RuntimeError) as raised:
        module.delete_flow(_entity("flow-pg", "Flow", "root", None, revision=1))

    message = str(raised.value)
    assert message.endswith("HTTP 409 Conflict")
    assert "Set-Cookie" not in message
    assert "sensitive" not in message


def test_format_safe_exception_sanitizes_regex_fallback_for_header_only_errors(import_cd_module, fake_nipyapi):
    module = _load_manage_flows(import_cd_module, fake_nipyapi)

    message = module.format_safe_exception(_HeaderBodyOnlyError())

    assert "Set-Cookie" not in message
    assert "Authorization" not in message
    assert "sensitive body" not in message
    assert message == "_HeaderBodyOnlyError"


def test_queue_drop_failure_cleans_request_and_blocks_delete(import_cd_module, fake_nipyapi):
    calls = _configure_delete_apis(fake_nipyapi, processors=[[]], controller_services=[[]])
    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_drop_all_flowfiles_request"] = (
        lambda **kwargs: types.SimpleNamespace(
            drop_request=types.SimpleNamespace(
                id="drop-1", finished=True, failure_reason="queue failure", state="FAILURE"
            )
        )
    )
    module = _load_manage_flows(import_cd_module, fake_nipyapi)

    with pytest.raises(RuntimeError, match="queue failure"):
        module.delete_flow(_entity("flow-pg", "Flow", "root", None, revision=1))

    assert ("remove-drop", "drop-1") in calls
    assert all(call[0] != "remove-pg" for call in calls)


def test_remove_process_group_retries_one_revision_conflict(import_cd_module, fake_nipyapi):
    class RevisionConflict(Exception):
        status = 409
        reason = "Conflict"
        body = "Revision version conflict"

    revisions = iter([3, 4])
    remove_versions = []
    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_process_group"] = (
        lambda **kwargs: _entity(kwargs["id"], "Flow", "root", None, revision=next(revisions))
    )

    def remove_process_group(**kwargs):
        remove_versions.append(kwargs["version"])
        if len(remove_versions) == 1:
            raise RevisionConflict()

    fake_nipyapi.api_methods["ProcessGroupsApi"]["remove_process_group"] = remove_process_group
    module = _load_manage_flows(import_cd_module, fake_nipyapi)
    module._await_deletable_flow_state = lambda *args, **kwargs: None

    module._remove_process_group("flow-pg", "Flow")

    assert remove_versions == ["3", "4"]


def test_delete_flows_wraps_configure_nifi_errors(import_cd_module, fake_nipyapi):
    fake_nipyapi.security.service_login = lambda **kwargs: (_ for _ in ()).throw(_HeaderBodyOnlyError())
    module = _load_manage_flows(import_cd_module, fake_nipyapi)

    with pytest.raises(RuntimeError, match="Failed to configure NiFi client for flow delete") as raised:
        module.delete_flows(
            [{"name": "Flow"}],
            "registry",
            "https://example.invalid/nifi-api",
            "",
            nifi_auth={"type": "username_password", "username": "user", "password": "pass"},
        )

    message = str(raised.value)
    assert "Set-Cookie" not in message
    assert "Authorization" not in message
    assert "sensitive body" not in message


def test_delete_flows_wraps_lookup_errors(import_cd_module, fake_nipyapi):
    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_process_groups"] = (
        lambda *args, **kwargs: (_ for _ in ()).throw(_HeaderBodyOnlyError())
    )
    module = _load_manage_flows(import_cd_module, fake_nipyapi)

    with pytest.raises(RuntimeError, match="Failed to locate process group 'Flow' for delete") as raised:
        module.delete_flows(
            [{"name": "Flow"}],
            "registry",
            "https://example.invalid/nifi-api",
            "token",
        )

    message = str(raised.value)
    assert "Set-Cookie" not in message
    assert "Authorization" not in message
    assert "sensitive body" not in message


def test_await_deletable_flow_state_wraps_listing_errors(import_cd_module, fake_nipyapi):
    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_processors"] = (
        lambda **kwargs: (_ for _ in ()).throw(_HeaderBodyOnlyError())
    )
    module = _load_manage_flows(import_cd_module, fake_nipyapi)

    with pytest.raises(RuntimeError, match="Failed to inspect deletable state for flow 'Flow'") as raised:
        module._await_deletable_flow_state("flow-pg", "Flow", timeout=0, interval=0)

    message = str(raised.value)
    assert "Set-Cookie" not in message
    assert "Authorization" not in message
    assert "sensitive body" not in message


def test_list_descendant_processors_uses_recursive_flag_when_supported(import_cd_module, fake_nipyapi):
    calls = []

    def get_processors(**kwargs):
        calls.append(kwargs)
        return types.SimpleNamespace(processors=[])

    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_processors"] = get_processors
    module = _load_manage_flows(import_cd_module, fake_nipyapi)

    module._list_descendant_processors("flow-pg")

    assert calls == [{"id": "flow-pg", "include_descendant_groups": True}]


def test_list_descendant_processors_retries_without_recursive_flag_for_legacy_clients(import_cd_module, fake_nipyapi):
    calls = []

    def get_processors(**kwargs):
        calls.append(kwargs)
        if "include_descendant_groups" in kwargs:
            raise TypeError("get_processors() got an unexpected keyword argument 'include_descendant_groups'")
        return types.SimpleNamespace(processors=[])

    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_processors"] = get_processors
    module = _load_manage_flows(import_cd_module, fake_nipyapi)

    module._list_descendant_processors("flow-pg")

    assert calls == [
        {"id": "flow-pg", "include_descendant_groups": True},
        {"id": "flow-pg"},
    ]


def test_list_descendant_processors_does_not_swallow_other_type_errors(import_cd_module, fake_nipyapi):
    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_processors"] = (
        lambda **kwargs: (_ for _ in ()).throw(TypeError("processor payload shape mismatch"))
    )
    module = _load_manage_flows(import_cd_module, fake_nipyapi)

    with pytest.raises(TypeError, match="processor payload shape mismatch"):
        module._list_descendant_processors("flow-pg")


def test_list_descendant_controller_services_uses_recursive_flags_when_supported(import_cd_module, fake_nipyapi):
    calls = []

    def get_controller_services_from_group(**kwargs):
        calls.append(kwargs)
        return types.SimpleNamespace(controller_services=[])

    fake_nipyapi.api_methods["FlowApi"]["get_controller_services_from_group"] = get_controller_services_from_group
    module = _load_manage_flows(import_cd_module, fake_nipyapi)

    module._list_descendant_controller_services("flow-pg")

    assert calls == [{
        "id": "flow-pg",
        "include_ancestor_groups": False,
        "include_descendant_groups": True,
    }]


def test_list_descendant_controller_services_retries_without_recursive_flags_for_legacy_clients(import_cd_module, fake_nipyapi):
    calls = []

    def get_controller_services_from_group(**kwargs):
        calls.append(kwargs)
        if "include_descendant_groups" in kwargs:
            raise TypeError("get_controller_services_from_group() got an unexpected keyword argument 'include_descendant_groups'")
        return types.SimpleNamespace(controller_services=[])

    fake_nipyapi.api_methods["FlowApi"]["get_controller_services_from_group"] = get_controller_services_from_group
    module = _load_manage_flows(import_cd_module, fake_nipyapi)

    module._list_descendant_controller_services("flow-pg")

    assert calls == [
        {
            "id": "flow-pg",
            "include_ancestor_groups": False,
            "include_descendant_groups": True,
        },
        {"id": "flow-pg"},
    ]


def test_list_descendant_controller_services_does_not_swallow_other_type_errors(import_cd_module, fake_nipyapi):
    fake_nipyapi.api_methods["FlowApi"]["get_controller_services_from_group"] = (
        lambda **kwargs: (_ for _ in ()).throw(TypeError("controller service payload shape mismatch"))
    )
    module = _load_manage_flows(import_cd_module, fake_nipyapi)

    with pytest.raises(TypeError, match="controller service payload shape mismatch"):
        module._list_descendant_controller_services("flow-pg")


def test_remove_process_group_raises_after_second_revision_conflict(import_cd_module, fake_nipyapi):
    class RevisionConflict(Exception):
        status = 409
        reason = "Conflict"
        body = "Revision update conflict"

    revisions = iter([3, 4])
    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_process_group"] = (
        lambda **kwargs: _entity(kwargs["id"], "Flow", "root", None, revision=next(revisions))
    )
    fake_nipyapi.api_methods["ProcessGroupsApi"]["remove_process_group"] = (
        lambda **kwargs: (_ for _ in ()).throw(RevisionConflict())
    )
    module = _load_manage_flows(import_cd_module, fake_nipyapi)
    module._await_deletable_flow_state = lambda *args, **kwargs: None

    with pytest.raises(RuntimeError, match="Failed to retry delete process group 'Flow': HTTP 409 Conflict"):
        module._remove_process_group("flow-pg", "Flow")


def test_remove_process_group_does_not_retry_non_revision_conflict(import_cd_module, fake_nipyapi):
    class RunningComponentsConflict(Exception):
        status = 409
        reason = "Conflict"
        body = '{"revision": {"version": 5}, "message": "Cannot delete process group with running components"}'

    attempts = []
    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_process_group"] = (
        lambda **kwargs: _entity(kwargs["id"], "Flow", "root", None, revision=3)
    )

    def remove_process_group(**kwargs):
        attempts.append(kwargs["version"])
        raise RunningComponentsConflict()

    fake_nipyapi.api_methods["ProcessGroupsApi"]["remove_process_group"] = remove_process_group
    module = _load_manage_flows(import_cd_module, fake_nipyapi)
    module._await_deletable_flow_state = lambda *args, **kwargs: None

    with pytest.raises(RuntimeError, match="Failed to delete process group 'Flow': HTTP 409 Conflict"):
        module._remove_process_group("flow-pg", "Flow")

    assert attempts == ["3"]