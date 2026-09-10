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

import nipyapi

from manage_assets import (
    _component_name,
    _entity_id,
    _is_revision_conflict,
    _reconcile_asset_reference,
    _submit_parameter_context_update,
    _validate_asset_specs,
)
from manage_flows import configure_nifi
from manage_parameters import _SECRET_RE, _find_parameter_context_by_name, resolve_value
from safe_exceptions import format_safe_exception


ROOT_PARAMETER_CONTEXT_NAME = "Root Parameter Context"


def _parameter_context_ref(context_id, context_name):
    return nipyapi.nifi.ParameterContextReferenceEntity(
        id=context_id,
        component=nipyapi.nifi.ParameterContextReferenceDTO(
            id=context_id,
            name=context_name,
        ),
    )


def _validate_root_parameter_context_spec(spec):
    assets = list((spec or {}).get("assets") or [])
    parameters = spec.get("parameters") or {}
    _validate_asset_specs(assets, "root process group")

    overlap = sorted({asset["parameter"] for asset in assets} & set(parameters))
    if overlap:
        raise RuntimeError(
            "[root-parameter-context] Parameters cannot also be asset targets in root process group: "
            + ", ".join(overlap)
        )


def _create_parameter_context(name):
    api = nipyapi.nifi.ParameterContextsApi()
    body = nipyapi.nifi.ParameterContextEntity(
        revision=nipyapi.nifi.RevisionDTO(version=0),
        component=nipyapi.nifi.ParameterContextDTO(name=name, parameters=[]),
    )
    try:
        context = api.create_parameter_context(body=body)
    except Exception as exc:
        raise RuntimeError(
            f"[root-parameter-context] Failed to create parameter context '{name}': {format_safe_exception(exc)}"
        ) from None
    print(f"[root-parameter-context] Created parameter context '{name}' (id={_entity_id(context)})")
    return context


def _get_root_process_group():
    root_id = nipyapi.canvas.get_root_pg_id()
    try:
        return nipyapi.nifi.ProcessGroupsApi().get_process_group(id=root_id)
    except Exception as exc:
        raise RuntimeError(
            f"[root-parameter-context] Failed to load root process group '{root_id}': {format_safe_exception(exc)}"
        ) from None


def _attach_parameter_context_to_process_group(pg, context):
    pg_api = nipyapi.nifi.ProcessGroupsApi()
    body = nipyapi.nifi.ProcessGroupEntity(
        id=_entity_id(pg),
        revision=pg.revision,
        component=nipyapi.nifi.ProcessGroupDTO(
            id=_entity_id(pg),
            parameter_context=_parameter_context_ref(_entity_id(context), _component_name(context)),
        ),
    )
    return pg_api.update_process_group(id=_entity_id(pg), body=body)


def _ensure_root_parameter_context():
    context = _find_parameter_context_by_name(ROOT_PARAMETER_CONTEXT_NAME)
    created = False
    if context is None:
        context = _create_parameter_context(ROOT_PARAMETER_CONTEXT_NAME)
        created = True

    for attempt in range(2):
        root_pg = _get_root_process_group()
        direct_context = getattr(root_pg.component, "parameter_context", None)
        if direct_context:
            return direct_context.id
        try:
            _attach_parameter_context_to_process_group(root_pg, context)
            print(
                f"[root-parameter-context] Attached parameter context '{_component_name(context)}' to root process group"
            )
            return _entity_id(context)
        except Exception as exc:
            if attempt == 0 and _is_revision_conflict(exc):
                continue
            action = "retry attach" if attempt else "attach"
            source = "created" if created else "existing"
            raise RuntimeError(
                "[root-parameter-context] Failed to "
                f"{action} {source} parameter context '{_component_name(context)}' to root process group: "
                f"{format_safe_exception(exc)}"
            ) from None
    return _entity_id(context)


def _resolve_root_context_id():
    root_pg = _get_root_process_group()
    direct_context = getattr(root_pg.component, "parameter_context", None)
    if direct_context:
        return direct_context.id
    return _ensure_root_parameter_context()


def _submit_root_context_update(context_id, body, action):
    _submit_parameter_context_update(context_id, body, action)
    api = nipyapi.nifi.ParameterContextsApi()
    return api.get_parameter_context(id=context_id)


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
    refreshed = _submit_root_context_update(context_id, body, action)
    direct = {p.parameter.name: p.parameter for p in (refreshed.component.parameters or [])}
    return refreshed, direct


def _create_missing_direct_parameter(context_id, context_name, param_name):
    def mutate(current):
        if param_name in current:
            return []
        return [
            nipyapi.nifi.ParameterDTO(
                name=param_name,
                value=None,
                sensitive=False,
                referenced_assets=[],
            )
        ]

    _update_direct_parameter_context(
        context_id,
        mutate,
        f"create parameter '{param_name}' in context '{context_name}'",
    )
    print(f"[root-parameter-context] Created direct asset parameter '{param_name}' in context '{context_name}'")


def _validate_direct_parameter(param, context_name, param_name):
    if getattr(param, "sensitive", False):
        raise RuntimeError(
            f"[root-parameter-context] Parameter '{param_name}' in context '{context_name}' is sensitive and cannot reference a root asset"
        )
    refs = list(getattr(param, "referenced_assets", None) or [])
    if len(refs) > 1:
        raise RuntimeError(
            f"[root-parameter-context] Parameter '{param_name}' in context '{context_name}' already references multiple assets"
        )
    value = getattr(param, "value", None)
    if not refs and value not in (None, ""):
        raise RuntimeError(
            f"[root-parameter-context] Parameter '{param_name}' in context '{context_name}' has a literal value and cannot be converted to an asset reference"
        )


def _ensure_direct_root_parameter(context_id, param_name):
    api = nipyapi.nifi.ParameterContextsApi()
    pc = api.get_parameter_context(id=context_id)
    context_name = pc.component.name
    direct = {p.parameter.name: p.parameter for p in (pc.component.parameters or [])}
    param = direct.get(param_name)
    if param is None:
        _create_missing_direct_parameter(context_id, context_name, param_name)
        pc = api.get_parameter_context(id=context_id)
        direct = {p.parameter.name: p.parameter for p in (pc.component.parameters or [])}
        param = direct[param_name]
    _validate_direct_parameter(param, context_name, param_name)
    return pc, param


def _resolve_provider_context_names(pattern, provider_context_names):
    names = list(provider_context_names or [])
    if not pattern:
        return []
    matches = [name for name in names if re.fullmatch(pattern, name)]
    if not matches:
        print(f"[root-parameter-context] No provider contexts matched pattern '{pattern}'")
    return matches


def _add_inherited_parameter_contexts(context_id, context_names):
    if not context_names:
        return

    api = nipyapi.nifi.ParameterContextsApi()
    pc = api.get_parameter_context(id=context_id)
    existing_inherited = list(pc.component.inherited_parameter_contexts or [])
    existing_ids = {ctx.id for ctx in existing_inherited}
    added = []

    for ctx_name in context_names:
        ctx = _find_parameter_context_by_name(ctx_name)
        if not ctx:
            print(f"[root-parameter-context] WARNING: parameter context '{ctx_name}' not found - skipping inheritance")
            continue
        if ctx.id in existing_ids:
            print(f"[root-parameter-context] '{ctx_name}' already inherited by '{pc.component.name}'")
            continue
        existing_inherited.append(_parameter_context_ref(ctx.id, ctx_name))
        existing_ids.add(ctx.id)
        added.append(ctx_name)

    if not added:
        print(f"[root-parameter-context] No new inherited contexts to add for '{pc.component.name}'")
        return

    body = nipyapi.nifi.ParameterContextEntity(
        id=context_id,
        revision=pc.revision,
        component=nipyapi.nifi.ParameterContextDTO(
            id=context_id,
            name=pc.component.name,
            parameters=[],
            inherited_parameter_contexts=existing_inherited,
        ),
    )
    _submit_root_context_update(context_id, body, f"add inherited contexts to '{pc.component.name}'")
    print(f"[root-parameter-context] Added inherited contexts {added} to '{pc.component.name}'")


def _desired_parameter_dto(name, value):
    kwargs = {"name": name, "value": value, "sensitive": False}
    if value is None:
        kwargs["value_removed"] = True
    return nipyapi.nifi.ParameterDTO(**kwargs)


def _plan_direct_parameter_changes(context_id, desired_parameters):
    if not desired_parameters:
        return {}

    api = nipyapi.nifi.ParameterContextsApi()
    pc = api.get_parameter_context(id=context_id)
    current = {p.parameter.name: p.parameter for p in (pc.component.parameters or [])}

    for name in desired_parameters:
        existing = current.get(name)
        if existing is None:
            continue
        if getattr(existing, "sensitive", False):
            raise RuntimeError(
                f"[root-parameter-context] Parameter '{name}' in context '{pc.component.name}' is sensitive and cannot be managed directly"
            )
        if getattr(existing, "referenced_assets", None):
            raise RuntimeError(
                f"[root-parameter-context] Parameter '{name}' in context '{pc.component.name}' references assets and cannot be managed directly"
            )

    changes = {}
    for name, desired_value in desired_parameters.items():
        if desired_value is not None and _SECRET_RE.fullmatch(str(desired_value)):
            raise RuntimeError(
                "root_parameter_context.parameters are stored as non-sensitive NiFi parameters; use a provided parameter context for secrets"
            )
        resolved = resolve_value(desired_value)
        normalized = str(resolved) if resolved is not None else None
        existing = current.get(name)
        if existing is not None and getattr(existing, "value", None) == normalized:
            print(f"[root-parameter-context] Parameter '{name}' already at desired value")
            continue
        changes[name] = normalized

    return changes


def _reconcile_direct_parameters(context_id, desired_parameters, planned_changes=None):
    if not desired_parameters:
        return

    changes = dict(planned_changes or {})
    if not changes:
        changes = _plan_direct_parameter_changes(context_id, desired_parameters)

    if not changes:
        api = nipyapi.nifi.ParameterContextsApi()
        pc = api.get_parameter_context(id=context_id)
        print(f"[root-parameter-context] Direct parameters already up-to-date for '{pc.component.name}'")
        return

    api = nipyapi.nifi.ParameterContextsApi()
    pc = api.get_parameter_context(id=context_id)
    current = {p.parameter.name: p.parameter for p in (pc.component.parameters or [])}
    updated = [_desired_parameter_dto(name, value) for name, value in changes.items()]

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
    _submit_root_context_update(context_id, body, f"update direct parameters in '{pc.component.name}'")
    print(f"[root-parameter-context] Updated {len(changes)} direct parameter(s) in '{pc.component.name}'")


def _reconcile_root_assets(context_id, assets_spec):
    for asset_spec in assets_spec:
        asset_name = asset_spec["name"]
        asset_url = asset_spec["url"]
        param_name = asset_spec["parameter"]
        _pc, param_dto = _ensure_direct_root_parameter(context_id, param_name)
        _reconcile_asset_reference(context_id, param_name, param_dto, asset_name, asset_url)


def reconcile_root_parameter_context(spec, runtime_url, nifi_pat, nifi_auth=None, provider_context_names=None):
    if not spec:
        return

    configure_nifi(runtime_url, pat=nifi_pat, nifi_auth=nifi_auth)
    _validate_root_parameter_context_spec(spec)
    context_id = _resolve_root_context_id()
    planned_parameter_changes = _plan_direct_parameter_changes(context_id, spec.get("parameters") or {})

    pattern = spec.get("provided_parameter_contexts")
    matched_context_names = _resolve_provider_context_names(pattern, provider_context_names)
    _add_inherited_parameter_contexts(context_id, matched_context_names)

    _reconcile_direct_parameters(context_id, spec.get("parameters") or {}, planned_changes=planned_parameter_changes)
    _reconcile_root_assets(context_id, spec.get("assets") or [])