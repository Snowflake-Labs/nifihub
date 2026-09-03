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

import importlib
import inspect
import os
import sys
import types

import pytest


SCRIPTS_CD_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

if SCRIPTS_CD_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_CD_DIR)


class _Model:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class _ReferenceTypeDTO:
    def __init__(self, reference_type=None):
        self._reference_type = reference_type

    @property
    def reference_type(self):
        return self._reference_type

    @reference_type.setter
    def reference_type(self, value):
        self._reference_type = value


class _ParameterGroupConfigurationEntity:
    def __init__(self, parameter_sensitivities=None):
        self._parameter_sensitivities = parameter_sensitivities

    @property
    def parameter_sensitivities(self):
        return self._parameter_sensitivities

    @parameter_sensitivities.setter
    def parameter_sensitivities(self, value):
        self._parameter_sensitivities = value


def _build_fake_nipyapi_module():
    allowed_api_methods = {
        "FlowApi": {
            "get_controller_services_from_group",
            "get_controller_services_from_controller",
            "get_parameter_providers",
            "get_versions",
            "schedule_components",
            "activate_controller_services",
        },
        "ControllerApi": {
            "create_controller_service",
            "get_flow_registry_clients",
            "create_parameter_provider",
        },
        "ControllerServicesApi": {
            "get_controller_service",
            "update_controller_service",
            "update_run_status1",
            "update_run_status2",
            "remove_controller_service",
        },
        "ProcessGroupsApi": {
            "get_process_group",
            "get_process_groups",
            "get_processors",
            "create_controller_service",
            "create_process_group",
            "update_process_group",
            "create_empty_all_connections_request",
            "get_drop_all_flowfiles_request",
            "remove_drop_request1",
            "remove_process_group",
        },
        "ProcessorsApi": {
            "get_processor",
            "update_processor",
            "update_run_status4",
        },
        "ParameterProvidersApi": {
            "update_parameter_provider",
            "fetch_parameters",
        },
        "ParameterContextsApi": {
            "get_parameter_context",
        },
    }
    api_methods = {
        "FlowApi": {},
        "ControllerApi": {},
        "ControllerServicesApi": {},
        "ProcessGroupsApi": {},
        "ProcessorsApi": {},
        "ParameterProvidersApi": {},
        "ParameterContextsApi": {},
    }

    def _api_class(api_name):
        class _Api:
            def __getattr__(self, item):
                if item not in allowed_api_methods[api_name]:
                    raise AssertionError(f"Unexpected fake {api_name} method: {item}")
                if item not in api_methods[api_name]:
                    raise AttributeError(item)
                return lambda *args, **kwargs: api_methods[api_name][item](*args, **kwargs)

        return _Api

    module = types.ModuleType("nipyapi")
    module.api_methods = api_methods
    module.config = types.SimpleNamespace(
        nifi_config=types.SimpleNamespace(host="", api_client=None, api_key={}, verify_ssl=True)
    )
    module.security = types.SimpleNamespace(
        service_login=lambda **kwargs: None,
        set_service_auth_token=lambda **kwargs: None,
    )
    module.canvas = types.SimpleNamespace(
        get_root_pg_id=lambda: "root",
        schedule_process_group=lambda *args, **kwargs: None,
    )
    module.layout = types.SimpleNamespace(suggest_pg_position=lambda parent_id: (0.0, 0.0))
    module.versioning = types.SimpleNamespace(
        revert_flow_ver=lambda *args, **kwargs: None,
        update_git_flow_ver=lambda *args, **kwargs: None,
    )

    nifi = types.SimpleNamespace(
        FlowApi=_api_class("FlowApi"),
        ControllerApi=_api_class("ControllerApi"),
        ControllerServicesApi=_api_class("ControllerServicesApi"),
        ProcessGroupsApi=_api_class("ProcessGroupsApi"),
        ProcessorsApi=_api_class("ProcessorsApi"),
        ParameterProvidersApi=_api_class("ParameterProvidersApi"),
        ParameterContextsApi=_api_class("ParameterContextsApi"),
        ControllerServiceReferencingComponentDTO=_ReferenceTypeDTO,
        ParameterGroupConfigurationEntity=_ParameterGroupConfigurationEntity,
    )
    for model_name in [
        "ControllerServiceEntity",
        "RevisionDTO",
        "ControllerServiceDTO",
        "BundleDTO",
        "ControllerServiceRunStatusEntity",
        "ProcessorEntity",
        "ProcessorDTO",
        "ProcessorConfigDTO",
        "ProcessorRunStatusEntity",
        "ActivateControllerServicesEntity",
        "ProcessGroupEntity",
        "ProcessGroupDTO",
        "PositionDTO",
        "VersionControlInformationDTO",
    ]:
        setattr(nifi, model_name, type(model_name, (_Model,), {}))
    module.nifi = nifi
    module.allowed_api_methods = allowed_api_methods
    return module


if "nipyapi" not in sys.modules:
    sys.modules["nipyapi"] = _build_fake_nipyapi_module()


@pytest.fixture
def fake_nipyapi(monkeypatch):
    module = _build_fake_nipyapi_module()
    monkeypatch.setitem(sys.modules, "nipyapi", module)
    return module


@pytest.fixture(autouse=True)
def cd_sys_path(monkeypatch):
    if SCRIPTS_CD_DIR not in sys.path:
        monkeypatch.syspath_prepend(SCRIPTS_CD_DIR)


@pytest.fixture
def import_cd_module(monkeypatch):
    imported = []

    def _import(module_name, stub_modules=None):
        for name, module in (stub_modules or {}).items():
            monkeypatch.setitem(sys.modules, name, module)
        for existing in list(sys.modules):
            if existing == module_name or existing.startswith(f"{module_name}."):
                sys.modules.pop(existing, None)
        module = importlib.import_module(module_name)
        imported.append(module_name)
        return module

    return _import


@pytest.fixture
def real_nipyapi_method_names():
    fake_module = sys.modules.pop("nipyapi", None)
    try:
        import nipyapi as real_nipyapi  # type: ignore
    except Exception:
        if fake_module is not None:
            sys.modules["nipyapi"] = fake_module
        return {}

    try:
        method_names = {}
        for api_name in (
            "ProcessorsApi",
            "ControllerServicesApi",
        ):
            api_cls = getattr(real_nipyapi.nifi, api_name, None)
            if api_cls is None:
                continue
            method_names[api_name] = {
                name for name, value in inspect.getmembers(api_cls) if callable(value) and not name.startswith("_")
            }
        return method_names
    finally:
        for module_name in list(sys.modules):
            if module_name == "nipyapi" or module_name.startswith("nipyapi."):
                sys.modules.pop(module_name, None)
        if fake_module is not None:
            sys.modules["nipyapi"] = fake_module