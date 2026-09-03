#!/usr/bin/env python3
# Copyright 2026 Snowflake Inc.
# SPDX-License-Identifier: Apache-2.0

import json
import os
import sys
import types

import pytest
import yaml


def _write_config(tmp_path, config):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return config_path


def _url_only_config():
    return {
        "account": {"name": "local", "github_environment": "local"},
        "deployments": [{
            "name": "LOCAL_DEPLOYMENT",
            "deployment_type": "SNOWFLAKE",
            "runtimes": [{
                "name": "LOCAL_NIFI",
                "database": "",
                "schema": "",
                "url": "https://localhost:8443",
                "nifi_auth": {
                    "type": "username_password",
                    "username": "admin",
                    "password": "admin",
                    "verify_ssl": False,
                },
                "flows": [],
            }],
        }],
    }


def _som_config():
    return {
        "account": {"name": "snowflake", "github_environment": "prod"},
        "deployments": [{
            "name": "DEPLOYMENT",
            "deployment_type": "SNOWFLAKE",
            "runtimes": [{
                "name": "RUNTIME",
                "database": "DB",
                "schema": "SCHEMA",
                "flows": [],
            }],
        }],
    }


def _mixed_config():
    config = _som_config()
    config["deployments"][0]["runtimes"].append({
        "name": "LOCAL_NIFI",
        "database": "",
        "schema": "",
        "url": "https://localhost:8443",
        "flows": [],
    })
    return config


def test_runtime_mode_helper_handles_empty_and_url_only(import_cd_module):
    module = import_cd_module("config_runtime_mode")

    assert module.all_configured_runtimes_are_url_managed({}) is False
    assert module.all_configured_runtimes_are_url_managed({"deployments": [{"name": "EMPTY", "runtimes": []}]}) is False
    assert module.all_configured_runtimes_are_url_managed(_url_only_config()) is True
    assert module.all_configured_runtimes_are_url_managed(_mixed_config()) is False


def test_describe_live_state_main_skips_conn_for_url_only(tmp_path, monkeypatch, import_cd_module, capsys):
    config_path = _write_config(tmp_path, _url_only_config())
    stubs = {
        "describe_nifi_state": types.SimpleNamespace(
            describe_nifi_state=lambda *args, **kwargs: {"flows": [], "flow_registries": []}
        ),
        "manage_connectors": types.SimpleNamespace(get_connector_config=lambda *args, **kwargs: pytest.fail("connector config should not be fetched")),
        "manage_flows": types.SimpleNamespace(configure_nifi=lambda *args, **kwargs: None),
    }
    module = import_cd_module("describe_live_state", stubs)

    for var in ("SNOWFLAKE_ACCOUNT_URL", "SNOWFLAKE_PAT", "SNOWFLAKE_USER", "SNOWFLAKE_ROLE"):
        monkeypatch.delenv(var, raising=False)

    monkeypatch.setattr(module, "_conn", lambda: pytest.fail("_conn should not be called for URL-only configs"))
    monkeypatch.setattr(module, "list_deployments", lambda conn: pytest.fail("Snowflake deployments should not be listed"))
    monkeypatch.setattr(module, "list_runtimes", lambda conn: pytest.fail("Snowflake runtimes should not be listed"))
    monkeypatch.setattr(module, "list_connectors", lambda conn: pytest.fail("Snowflake connectors should not be listed"))
    monkeypatch.setattr(sys, "argv", ["describe_live_state.py", str(config_path)])

    module.main()

    state = json.loads(capsys.readouterr().out)
    runtime = state["deployments"][0]["runtimes"][0]
    assert runtime["name"] == "LOCAL_NIFI"
    assert runtime["nifi"] == {"flows": [], "flow_registries": []}


@pytest.mark.parametrize("config_factory", [_som_config, _mixed_config])
def test_describe_live_state_main_uses_conn_when_not_all_url_managed(tmp_path, monkeypatch, import_cd_module, config_factory):
    config_path = _write_config(tmp_path, config_factory())
    stubs = {
        "describe_nifi_state": types.SimpleNamespace(describe_nifi_state=lambda *args, **kwargs: {}),
        "manage_connectors": types.SimpleNamespace(get_connector_config=lambda *args, **kwargs: {}),
        "manage_flows": types.SimpleNamespace(configure_nifi=lambda *args, **kwargs: None),
    }
    module = import_cd_module("describe_live_state", stubs)

    sentinel_conn = {"account_url": "https://acct", "pat": "pat", "user": "user", "role": "OPENFLOW_ADMIN"}
    calls = []

    monkeypatch.setattr(module, "_conn", lambda: calls.append("conn") or sentinel_conn)

    def fake_build_live_state(path, conn=None):
        calls.append((path, conn))
        assert conn == sentinel_conn
        return {"deployments": []}

    monkeypatch.setattr(module, "build_live_state", fake_build_live_state)
    monkeypatch.setattr(sys, "argv", ["describe_live_state.py", str(config_path)])

    module.main()

    assert calls == ["conn", (str(config_path), sentinel_conn)]


def test_validate_pr_skips_snowflake_for_url_only_config(tmp_path, monkeypatch, import_cd_module, capsys):
    config_path = _write_config(tmp_path, _url_only_config())
    schema_path = os.path.join(tmp_path, "schema.json")
    schema = {
        "type": "object",
        "required": ["account", "deployments"],
        "properties": {
            "account": {"type": "object"},
            "deployments": {"type": "array"},
        },
    }
    with open(schema_path, "w", encoding="utf-8") as handle:
        json.dump(schema, handle)

    module = import_cd_module("validate_pr")
    for var in ("SNOWFLAKE_ACCOUNT_URL", "SNOWFLAKE_PAT", "SNOWFLAKE_USER", "SNOWFLAKE_ROLE"):
        monkeypatch.delenv(var, raising=False)

    monkeypatch.setattr(module, "check_connectivity", lambda conn: pytest.fail("connectivity should be skipped"))
    monkeypatch.setattr(module, "describe_deployment", lambda *args, **kwargs: pytest.fail("deployment describe should be skipped"))
    monkeypatch.setattr(module, "list_runtimes", lambda *args, **kwargs: pytest.fail("runtime listing should be skipped"))
    monkeypatch.setattr(sys, "argv", ["validate_pr.py", str(config_path), schema_path])

    module.main()

    out = capsys.readouterr().out
    assert "Validation passed." in out
    comment = open("/tmp/pr-comment.md", encoding="utf-8").read()
    assert "Skipped Snowflake validation because every configured runtime is URL-managed" in comment
    assert "Skipped deployment and runtime inventory" in comment


@pytest.mark.parametrize("config_factory", [_som_config, _mixed_config])
def test_validate_pr_requires_snowflake_when_not_all_url_managed(tmp_path, monkeypatch, import_cd_module, config_factory):
    config_path = _write_config(tmp_path, config_factory())
    schema_path = os.path.join(tmp_path, "schema.json")
    schema = {
        "type": "object",
        "required": ["account", "deployments"],
        "properties": {
            "account": {"type": "object"},
            "deployments": {"type": "array"},
        },
    }
    with open(schema_path, "w", encoding="utf-8") as handle:
        json.dump(schema, handle)

    module = import_cd_module("validate_pr")
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT_URL", "https://acct.snowflakecomputing.com")
    monkeypatch.setenv("SNOWFLAKE_PAT", "pat")
    monkeypatch.setenv("SNOWFLAKE_USER", "user")
    monkeypatch.setenv("SNOWFLAKE_ROLE", "OPENFLOW_ADMIN")
    calls = []

    def fake_check_connectivity(conn):
        calls.append(conn)
        return True, None

    monkeypatch.setattr(module, "check_connectivity", fake_check_connectivity)
    monkeypatch.setattr(module, "describe_deployment", lambda *args, **kwargs: (None, "does not exist"))
    monkeypatch.setattr(module, "list_runtimes", lambda *args, **kwargs: ([], None))
    monkeypatch.setattr(sys, "argv", ["validate_pr.py", str(config_path), schema_path])

    module.main()

    assert calls == [{
        "account_url": "https://acct.snowflakecomputing.com",
        "pat": "pat",
        "user": "user",
        "role": "OPENFLOW_ADMIN",
    }]