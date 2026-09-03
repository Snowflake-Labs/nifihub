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


_HTTP_RESPONSE_HEADERS_RE = re.compile(
    r"HTTP response headers:.*?(?=HTTP response body:|$)",
    re.IGNORECASE | re.DOTALL,
)
_HTTP_RESPONSE_BODY_RE = re.compile(
    r"HTTP response body:.*$",
    re.IGNORECASE | re.DOTALL,
)
_SENSITIVE_HEADER_RE = re.compile(
    r"(?i)\b(set-cookie|cookie|authorization)\b\s*:[^\n]*"
)


def format_safe_exception(exc):
    status = getattr(exc, "status", None)
    reason = getattr(exc, "reason", None)
    if status is not None or reason:
        if status is not None and reason:
            return f"HTTP {status} {reason}"
        if status is not None:
            return f"HTTP {status}"
        return str(reason)

    message = str(exc or "")
    if message:
        message = _HTTP_RESPONSE_HEADERS_RE.sub("", message)
        message = _HTTP_RESPONSE_BODY_RE.sub("", message)
        message = _SENSITIVE_HEADER_RE.sub(r"\1: [redacted]", message)
        message = " ".join(message.split())
    return message or exc.__class__.__name__