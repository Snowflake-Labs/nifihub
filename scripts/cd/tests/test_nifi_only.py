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

import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import types
import xml.etree.ElementTree as ET

import pytest
import yaml
from jsonschema import Draft7Validator

ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture(autouse=True)
def clean_auth_environment(monkeypatch):
    for name in ("NIFI_USERNAME", "NIFI_PASSWORD", "NIFI_TOKEN", "NIFI_VERIFY_SSL"):
        monkeypatch.delenv(name, raising=False)


def config_file(tmp_path, runtimes=None, **extra):
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump({
        "name": "test", "runtimes": runtimes if runtimes is not None else [{"name": "primary", "url": "https://nifi.example.test:8443"}], **extra,
    }))
    return path


def test_environment_schema_rejects_infrastructure_and_requires_explicit_url(tmp_path, import_cd_module):
    module = import_cd_module("validate_pr")
    for extra in ({"deployments": []}, {"account": {}}):
        path = config_file(tmp_path, **extra)
        assert module.validate_schema(path, ROOT / "environments/schema.json")[1]
    for runtime in ({"name": "primary"}, {"name": "primary", "url": "https://nifi.example.test", "node_type": "SMALL"}):
        path = config_file(tmp_path, [runtime])
        assert module.validate_schema(path, ROOT / "environments/schema.json")[1]


def test_duplicate_instance_names_and_urls_are_rejected(tmp_path, import_cd_module):
    module = import_cd_module("nifi_config")
    for runtimes in (
        [{"name": "primary", "url": "https://one.example.test"}, {"name": "PRIMARY", "url": "https://two.example.test"}],
        [{"name": "one", "url": "https://nifi.example.test/nifi"}, {"name": "two", "url": "https://nifi.example.test/nifi-api/"}],
    ):
        with pytest.raises(ValueError, match="unique|only once"):
            module.load_config(config_file(tmp_path, runtimes))


@pytest.mark.parametrize("suffix", ["", "/", "/nifi", "/nifi-api/"])
def test_connection_normalizes_url_and_resets_auth_between_instances(suffix, fake_nipyapi, import_cd_module, monkeypatch):
    module = import_cd_module("manage_flows")
    fake_nipyapi.config.nifi_config.api_key["bearerAuth"] = "Bearer old-instance-token"
    fake_nipyapi.config.nifi_config.verify_ssl = False
    monkeypatch.setenv("NIFI_VERIFY_SSL", "/tmp/private-ca.pem")
    module.configure_nifi("https://nifi.example.test:8443" + suffix)
    assert fake_nipyapi.config.nifi_config.host == "https://nifi.example.test:8443/nifi-api"
    assert "bearerAuth" not in fake_nipyapi.config.nifi_config.api_key
    assert fake_nipyapi.config.nifi_config.verify_ssl is True
    assert fake_nipyapi.config.nifi_config.ssl_ca_cert == "/tmp/private-ca.pem"


@pytest.mark.parametrize("source", ["environment", "runtime"])
def test_custom_ca_reaches_real_client_and_resets_between_instances(source, tmp_path):
    # Use a separate interpreter because the unit-test fixtures replace nipyapi.
    result = subprocess.run([sys.executable, "-c", """
import os
import ssl
import sys
import certifi
sys.path.insert(0, "scripts/cd")
import nipyapi
from nipyapi.nifi.rest import RESTClientObject
from manage_flows import configure_nifi

ca_path, source = sys.argv[1:]
os.environ["NIFI_VERIFY_SSL"] = ca_path if source == "environment" else "false"
auth = None
if source == "runtime":
    nipyapi.security.service_login = lambda **kwargs: True
    auth = {"type": "username_password", "username": "test", "password": "test", "verify_ssl": ca_path}
configure_nifi("https://private.example.test", nifi_auth=auth)
pool = RESTClientObject().pool_manager.connection_pool_kw
assert pool["ca_certs"] == ca_path, pool
assert pool["cert_reqs"] == ssl.CERT_REQUIRED, pool

os.environ["NIFI_VERIFY_SSL"] = "true"
configure_nifi("https://public.example.test")
pool = RESTClientObject().pool_manager.connection_pool_kw
assert pool["ca_certs"] == certifi.where(), pool
assert pool["cert_reqs"] == ssl.CERT_REQUIRED, pool
assert nipyapi.config.nifi_config.ssl_ca_cert is None

os.environ["NIFI_VERIFY_SSL"] = "false"
configure_nifi("http://localhost:8080")
pool = RESTClientObject().pool_manager.connection_pool_kw
assert pool["cert_reqs"] == ssl.CERT_NONE, pool
assert pool["ca_certs"] is None, pool
""", str(tmp_path / "private-ca.pem"), source], cwd=ROOT, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("url", ["file:///tmp/nifi", "https://user:password@nifi.example.test", "https://nifi.example.test?token=secret"])
def test_connection_rejects_unsafe_urls_before_setting_client(url, fake_nipyapi, import_cd_module):
    module = import_cd_module("manage_flows")
    with pytest.raises(ValueError, match="HTTP"):
        module.configure_nifi(url)
    assert fake_nipyapi.config.nifi_config.host == ""


def test_credentials_require_https_and_login_failure_is_fatal(fake_nipyapi, import_cd_module):
    module = import_cd_module("manage_flows")
    with pytest.raises(ValueError, match="HTTPS"):
        module.configure_nifi("http://localhost:8080", pat="token")
    fake_nipyapi.security.service_login = lambda **kwargs: False
    with pytest.raises(RuntimeError, match="login failed"):
        module.configure_nifi("https://localhost:8443", nifi_auth={"type": "username_password", "username": "admin", "password": "invalid"})


def test_username_password_environment_and_explicit_auth_resolution(fake_nipyapi, import_cd_module, monkeypatch):
    import_cd_module("manage_flows")
    import_cd_module("manage_parameters")
    module = import_cd_module("nifi_config")
    monkeypatch.setenv("NIFI_USERNAME", "admin")
    with pytest.raises(ValueError, match="both"):
        module.get_nifi_auth()
    monkeypatch.setenv("NIFI_PASSWORD", "password")
    assert module.get_nifi_auth()["username"] == "admin"
    monkeypatch.setenv("GH_VARS_JSON", json.dumps({"USER": "other-user"}))
    monkeypatch.setenv("GH_SECRETS_JSON", json.dumps({"PASSWORD": "other-password"}))
    auth = module.get_nifi_auth({"nifi_auth": {
        "type": "username_password", "username": "${{ vars.USER }}", "password": "${{ secrets.PASSWORD }}", "verify_ssl": False,
    }})
    assert auth == {"type": "username_password", "username": "other-user", "password": "other-password", "verify_ssl": False}


def test_describe_skips_disabled_instance_and_keeps_json_clean(tmp_path, import_cd_module, monkeypatch, capsys):
    calls = []
    def describe(url, **kwargs):
        calls.append((url, kwargs))
        print("diagnostic output")
        return {"flows": [], "parameters": {}}
    module = import_cd_module("describe_live_state", {"describe_nifi_state": types.SimpleNamespace(describe_nifi_state=describe)})
    path = config_file(tmp_path, [
        {"name": "primary", "url": "https://one.example.test"},
        {"name": "disabled", "url": "https://two.example.test", "reconcile": False},
    ])
    monkeypatch.setattr(sys, "argv", ["describe_live_state.py", str(path)])
    module.main()
    captured = capsys.readouterr()
    state = json.loads(captured.out)
    assert len(calls) == 1
    assert state["runtimes"][1]["nifi"] == {"skipped": True}
    assert "diagnostic output" in captured.err


def test_unreadable_state_blocks_translation(tmp_path, import_cd_module):
    diff = import_cd_module("diff_live")
    translate = import_cd_module("translate_live_diff")
    path = config_file(tmp_path)
    for state in ({"runtimes": []}, {"runtimes": [{"name": "primary", "nifi": {"error": "HTTP 403"}}]}):
        with pytest.raises(RuntimeError, match="refusing to apply"):
            translate.translate(diff.diff_live(state, path))


def test_omitted_scopes_are_preserved_but_declared_empty_scopes_are_pruned(import_cd_module):
    module = import_cd_module("diff_live")
    live = {"flows": [{"name": "Unrelated Flow"}], "controller_services": [{"name": "Unrelated Service"}]}
    omitted = module.diff_nifi_state(live, {})
    assert omitted["flows"] == {}
    assert omitted["controller_services"] == {}
    declared = module.diff_nifi_state(live, {"flows": [], "controller_services": []})
    assert declared["flows"]["deleted"] == live["flows"]
    assert declared["controller_services"]["deleted"] == live["controller_services"]


def test_unchanged_declared_resources_still_reconcile_secrets_and_latest_versions(tmp_path, import_cd_module):
    diff = import_cd_module("diff_live")
    translate = import_cd_module("translate_live_diff")
    path = config_file(tmp_path)
    state = {"runtimes": [{"name": "primary", "nifi": {"flows": [], "parameters": {}}}]}
    plan = diff.diff_live(state, path)
    assert len(plan["runtimes"]["unchanged"]) == 1
    assert translate.translate(plan)["runtimes"]["modified"][0]["new"]["url"] == "https://nifi.example.test:8443"


@pytest.fixture
def ci_deployer(fake_nipyapi, import_cd_module, monkeypatch):
    import_cd_module("manage_flows")
    import_cd_module("manage_parameters")
    monkeypatch.setattr(fake_nipyapi.nifi, "models", types.SimpleNamespace(
        ParameterGroupConfigurationEntity=fake_nipyapi.nifi.ParameterGroupConfigurationEntity,
    ), raising=False)
    import_cd_module("manage_parameter_providers")
    spec = importlib.util.spec_from_file_location("deploy_ci_flow", ROOT / "scripts/ci/deploy_ci_flow.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_ci_records_created_group_before_configuration_failure(tmp_path, ci_deployer, monkeypatch):
    module = ci_deployer
    monkeypatch.setattr(module, "configure_nifi", lambda *args, **kwargs: None)
    monkeypatch.setattr(module, "upload_flow", lambda *args, **kwargs: ("new-test-pg", "CI_test_unique"))
    monkeypatch.setattr(module, "reconcile_parameter_providers", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("Provider failed")))
    config = tmp_path / "test.yaml"
    config.write_text(yaml.safe_dump({"github_environment": "test", "parameter_providers": [{"name": "provider", "type": "example.Provider"}]}))
    output = tmp_path / "pg-id"
    with pytest.raises(RuntimeError, match="Provider failed"):
        module.deploy("unused.json", config, "https://nifi.example.test", output_file=output)
    assert output.read_text() == "new-test-pg"


def test_ci_parameter_contexts_are_isolated_including_nested_and_inherited_contexts(ci_deployer):
    flow = {
        "parameterContexts": {
            "Base": {"name": "Base"},
            "Flow": {"name": "Flow", "inheritedParameterContexts": ["Base", "External Provider Context"]},
        },
        "flowContents": {
            "parameterContextName": "Flow",
            "processGroups": [{"parameterContextName": "Base"}],
        },
    }
    ci_deployer._isolate_parameter_contexts(flow, "CI_unique")
    assert set(flow["parameterContexts"]) == {"CI_unique - Base", "CI_unique - Flow"}
    assert flow["parameterContexts"]["CI_unique - Flow"]["inheritedParameterContexts"] == ["CI_unique - Base", "External Provider Context"]
    assert flow["flowContents"]["parameterContextName"] == "CI_unique - Flow"
    assert flow["flowContents"]["processGroups"][0]["parameterContextName"] == "CI_unique - Base"


@pytest.mark.parametrize("fail_group_delete", [False, True])
def test_ci_cleanup_removes_only_owned_contexts_after_group_deletion(
    fail_group_delete, fake_nipyapi, import_cd_module, monkeypatch,
):
    import_cd_module("manage_flows")
    spec = importlib.util.spec_from_file_location("cleanup_flow_test", ROOT / "scripts/cleanup-flow.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "configure_nifi", lambda *args, **kwargs: None)
    monkeypatch.setattr(module, "get_nifi_auth", lambda: None)
    group = types.SimpleNamespace(component=types.SimpleNamespace(name="CI_unique"))
    contexts = {
        "flow": types.SimpleNamespace(id="flow", revision=types.SimpleNamespace(version=1), component=types.SimpleNamespace(
            name="CI_unique - Flow", inherited_parameter_contexts=[types.SimpleNamespace(id="base")],
        )),
        "base": types.SimpleNamespace(id="base", revision=types.SimpleNamespace(version=1), component=types.SimpleNamespace(
            name="CI_unique - Base", inherited_parameter_contexts=[],
        )),
        "other": types.SimpleNamespace(id="other", component=types.SimpleNamespace(name="Shared Provider Context")),
    }
    monkeypatch.setattr(fake_nipyapi.nifi, "ProcessGroupsApi", lambda: types.SimpleNamespace(get_process_group=lambda **kwargs: group))
    monkeypatch.setattr(fake_nipyapi.nifi, "FlowApi", lambda: types.SimpleNamespace(get_parameter_contexts=lambda: types.SimpleNamespace(parameter_contexts=list(contexts.values()))))
    calls = []
    monkeypatch.setattr(fake_nipyapi.nifi, "ParameterContextsApi", lambda: types.SimpleNamespace(
        get_parameter_context=lambda id: contexts[id],
        delete_parameter_context=lambda **kwargs: calls.append(kwargs["id"]),
    ))
    for method in ("schedule_all_controllers", "purge_process_group"):
        monkeypatch.setattr(fake_nipyapi.canvas, method, lambda *args, **kwargs: None, raising=False)
    def delete_group(*args, **kwargs):
        if fail_group_delete:
            raise RuntimeError("Group still in use")
        calls.append("group")
    monkeypatch.setattr(fake_nipyapi.canvas, "delete_process_group", delete_group, raising=False)
    if fail_group_delete:
        with pytest.raises(RuntimeError, match="still in use"):
            module.cleanup("test-group", "https://nifi.example.test")
        assert calls == []
    else:
        module.cleanup("test-group", "https://nifi.example.test")
        assert calls == ["group", "flow", "base"]


def test_all_examples_match_schemas_and_use_apache_processor_bundles(import_cd_module):
    module = import_cd_module("validate_pr")
    for path in (ROOT / "environments").glob("*/config.yaml"):
        assert module.validate_schema(path, ROOT / "environments/schema.json")[1] == []
    schema = json.loads((ROOT / "scripts/ci/ci-runtime-schema.json").read_text())
    for path in (ROOT / "flows").glob("*/tests/*.yaml"):
        assert list(Draft7Validator(schema).iter_errors(yaml.safe_load(path.read_text()))) == []
    namespace = {"m": "http://maven.apache.org/POM/4.0.0"}
    version = ET.parse(ROOT / "pom.xml").find("m:properties/m:nifi.version", namespace).text
    def check_components(value):
        if isinstance(value, dict):
            if value.get("bundle", {}).get("group") == "org.apache.nifi":
                assert value["bundle"]["version"] == version
            if "type" in value:
                assert not value["type"].startswith("com.snowflake.openflow.")
            for child in value.values():
                check_components(child)
        elif isinstance(value, list):
            for child in value:
                check_components(child)
    for path in (ROOT / "flows").glob("*/*.json"):
        check_components(json.loads(path.read_text()))


def test_environment_detection_reconciles_all_on_shared_changes(tmp_path, import_cd_module, monkeypatch):
    module = import_cd_module("detect_environments")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DIFF_BASE", "base-sha")
    monkeypatch.delenv("INPUT_ENVIRONMENT", raising=False)
    monkeypatch.delenv("MANUAL_RUN", raising=False)
    for name, github_environment in (("dev", "development"), ("prod", "production")):
        path = tmp_path / f"environments/{name}/config.yaml"
        path.parent.mkdir(parents=True)
        path.write_text(yaml.safe_dump({"github_environment": github_environment}))
    calls = []
    def changed(*args, **kwargs):
        calls.append(args[0])
        return types.SimpleNamespace(stdout="scripts/cd/orchestrate.py\n")
    monkeypatch.setattr(module.subprocess, "run", changed)
    output = tmp_path / "outputs"
    module.main(str(output))
    items = json.loads(next(line.removeprefix("items=") for line in output.read_text().splitlines() if line.startswith("items=")))
    assert items == [{"config_env": "dev", "github_env": "development"}, {"config_env": "prod", "github_env": "production"}]
    assert calls[0] == ["git", "diff", "--name-only", "base-sha", "HEAD"]


def test_manual_environment_selection_rejects_path_traversal(import_cd_module, monkeypatch):
    module = import_cd_module("detect_environments")
    monkeypatch.setenv("INPUT_ENVIRONMENT", "../outside")
    with pytest.raises(ValueError, match="Invalid environment"):
        module.main()


@pytest.mark.parametrize("mode", ["selected", "all", "push"])
def test_environment_detection_rejects_malformed_yaml(mode, tmp_path, import_cd_module, monkeypatch):
    module = import_cd_module("detect_environments")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("INPUT_ENVIRONMENT", "prod" if mode == "selected" else "")
    monkeypatch.setenv("MANUAL_RUN", "true" if mode == "all" else "false")
    monkeypatch.setenv("DIFF_BASE", "base-sha")
    path = tmp_path / "environments/prod/config.yaml"
    path.parent.mkdir(parents=True)
    path.write_text("runtimes: [\n")
    monkeypatch.setattr(module.subprocess, "run", lambda *args, **kwargs: types.SimpleNamespace(stdout="environments/prod/config.yaml\n"))
    output = tmp_path / "outputs"
    with pytest.raises(yaml.YAMLError):
        module.main(str(output))
    assert not output.exists()


def test_environment_detection_propagates_permission_errors(tmp_path, import_cd_module, monkeypatch):
    module = import_cd_module("detect_environments")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("INPUT_ENVIRONMENT", "")
    monkeypatch.setenv("MANUAL_RUN", "true")
    monkeypatch.setattr(module.glob, "glob", lambda pattern: ["environments/prod/config.yaml"])
    def denied(path, *args, **kwargs):
        if path == "environments/prod/config.yaml":
            raise PermissionError("Cannot read environment configuration")
        return open(path, *args, **kwargs)
    monkeypatch.setattr(module, "open", denied, raising=False)
    with pytest.raises(PermissionError):
        module.main(str(tmp_path / "outputs"))


@pytest.mark.parametrize("mode", ["selected", "all", "push"])
def test_only_automatic_detection_skips_deleted_configurations(mode, tmp_path, import_cd_module, monkeypatch):
    module = import_cd_module("detect_environments")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("INPUT_ENVIRONMENT", "prod" if mode == "selected" else "")
    monkeypatch.setenv("MANUAL_RUN", "true" if mode == "all" else "false")
    monkeypatch.setenv("DIFF_BASE", "base-sha")
    monkeypatch.setattr(module.glob, "glob", lambda pattern: ["environments/prod/config.yaml"])
    monkeypatch.setattr(module.subprocess, "run", lambda *args, **kwargs: types.SimpleNamespace(stdout="environments/prod/config.yaml\n"))
    output = tmp_path / "outputs"
    if mode == "push":
        module.main(str(output))
        assert output.read_text() == "has_changes=false\nitems=[]\n"
    else:
        with pytest.raises((ValueError, FileNotFoundError)):
            module.main(str(output))
        assert not output.exists()


@pytest.mark.parametrize("workflow,job", [("environment-cd.yml", None), ("flow-deploy.yml", "deploy")])
def test_deployment_concurrency_preserves_pending_requests(workflow, job):
    config = yaml.safe_load((ROOT / ".github/workflows" / workflow).read_text())
    concurrency = (config["jobs"][job] if job else config)["concurrency"]
    assert concurrency["queue"] == "max"
    assert concurrency["cancel-in-progress"] is False
