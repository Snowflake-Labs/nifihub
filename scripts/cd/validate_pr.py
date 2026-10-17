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
"""Validate an Apache NiFi environment configuration without contacting a server."""
import json
import re
import sys

import yaml
from jsonschema import Draft7Validator, FormatChecker

_SECRET_RE = re.compile(r'^\$\{\{\s*secrets\.(\w+)\s*\}\}$')


def validate_schema(config_path, schema_path):
    with open(schema_path) as handle:
        schema = json.load(handle)
    with open(config_path) as handle:
        data = yaml.safe_load(handle)
    errors = [error.message for error in Draft7Validator(schema, format_checker=FormatChecker()).iter_errors(data)]
    if not errors:
        names = [rt["name"].upper() for rt in data["runtimes"]]
        if len(names) != len(set(names)):
            errors.append("NiFi runtime names must be unique (case-insensitive).")
        urls = [rt["url"].rstrip("/").removesuffix("/nifi-api").removesuffix("/nifi") for rt in data["runtimes"]]
        if len(urls) != len(set(urls)):
            errors.append("Each NiFi URL must appear only once in an environment.")
        errors.extend(validate_root_parameter_context(data))
    return data, errors


def validate_root_parameter_context(config_data):
    errors = []
    for runtime in (config_data or {}).get("runtimes", []):
        spec = runtime.get("root_parameter_context") or {}
        assets = spec.get("assets", []) or []
        runtime_name = runtime.get("name", "<unnamed>")
        names = [asset.get("name") for asset in assets]
        parameters = [asset.get("parameter") for asset in assets]
        duplicate_names = sorted({name for name in names if name and names.count(name) > 1})
        duplicate_parameters = sorted({name for name in parameters if name and parameters.count(name) > 1})
        if duplicate_names:
            errors.append(
                f"Runtime '{runtime_name}' has duplicate root_parameter_context asset names: "
                + ", ".join(duplicate_names)
            )
        if duplicate_parameters:
            errors.append(
                f"Runtime '{runtime_name}' maps multiple root_parameter_context assets to the same parameter: "
                + ", ".join(duplicate_parameters)
            )
        overlap = sorted(set(parameters) & set((spec.get("parameters") or {}).keys()))
        if overlap:
            errors.append(
                f"Runtime '{runtime_name}' declares root_parameter_context parameters that are also asset targets: "
                + ", ".join(overlap)
            )
        for parameter_name, parameter_value in (spec.get("parameters") or {}).items():
            if parameter_value is not None and _SECRET_RE.fullmatch(str(parameter_value)):
                errors.append(
                    f"Runtime '{runtime_name}' root_parameter_context parameter '{parameter_name}' uses a GitHub secret reference, "
                    "but root_parameter_context.parameters are stored as non-sensitive NiFi parameters; use a provided parameter context for secrets"
                )
    return errors


def main():
    if len(sys.argv) != 3:
        sys.exit("Usage: validate_pr.py <config_path> <schema_path>")
    _, errors = validate_schema(sys.argv[1], sys.argv[2])
    body = "## Environment CD — Configuration Validation\n\n"
    body += "\n".join(f"- {error}" for error in errors) if errors else "Configuration is valid."
    with open("/tmp/pr-comment.md", "w") as handle:
        handle.write(body + "\n")
    if errors:
        sys.exit("Configuration validation failed — see /tmp/pr-comment.md.")
    print("Configuration validation passed.")


if __name__ == "__main__":
    main()
