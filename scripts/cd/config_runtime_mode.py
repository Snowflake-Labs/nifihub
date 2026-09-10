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

import yaml


def load_config(config_path):
    with open(config_path) as handle:
        return yaml.safe_load(handle) or {}


def iter_configured_runtimes(config):
    for deployment in config.get("deployments", []) or []:
        for runtime in deployment.get("runtimes", []) or []:
            yield runtime


def runtime_is_url_managed(runtime):
    return bool((runtime or {}).get("url"))


def deployment_is_url_managed(deployment):
    runtimes = deployment.get("runtimes", []) or []
    return bool(runtimes) and all(runtime_is_url_managed(runtime) for runtime in runtimes)


def all_configured_runtimes_are_url_managed(config):
    runtime_count = 0
    for runtime in iter_configured_runtimes(config):
        runtime_count += 1
        if not runtime_is_url_managed(runtime):
            return False
    return runtime_count > 0