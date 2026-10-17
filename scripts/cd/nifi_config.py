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

"""Shared configuration and authentication for existing NiFi instances."""
import os
from pathlib import Path


def load_config(config_path):
    from validate_pr import validate_schema
    schema_path = Path(__file__).resolve().parents[2] / "environments/schema.json"
    config, errors = validate_schema(config_path, schema_path)
    if errors:
        raise ValueError("Invalid NiFi environment configuration: " + "; ".join(errors))
    return config


def get_nifi_auth(runtime=None):
    from manage_parameters import resolve_value
    auth = (runtime or {}).get("nifi_auth")
    if auth:
        resolved = {
            "type": "username_password",
            "username": resolve_value(auth["username"]),
            "password": resolve_value(auth["password"]),
        }
        if "verify_ssl" in auth:
            resolved["verify_ssl"] = auth["verify_ssl"]
        return resolved
    username = os.environ.get("NIFI_USERNAME", "")
    password = os.environ.get("NIFI_PASSWORD", "")
    if username or password:
        if not username or not password:
            raise ValueError("Set both NIFI_USERNAME and NIFI_PASSWORD.")
        return {"type": "username_password", "username": username, "password": password}
    return None
