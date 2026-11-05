/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.snowflake.nifihub.data.snowflaketablecache;

import java.util.regex.Pattern;

/** The source scope and exact query boundary represented by the committed cache contents. */
public record CacheCheckpoint(String queryId, String sourceSignature) {

    private static final Pattern QUERY_ID_PATTERN = Pattern.compile("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");

    public CacheCheckpoint {
        if (queryId == null || queryId.isBlank() || !QUERY_ID_PATTERN.matcher(queryId).matches()) {
            throw new IllegalArgumentException("Checkpoint query ID must have hexadecimal UUID shape");
        }
        if (sourceSignature == null || sourceSignature.isBlank()) {
            throw new IllegalArgumentException("Checkpoint source signature must be nonblank");
        }
    }
}
