/*
 * Copyright 2025-2026 The trino-teradata-direct contributors
 *
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
package io.trino.plugin.teradata.export;

import io.trino.spi.Page;

public record BatchContainer(Page page, boolean isEndOfStream) {
    public static BatchContainer endOfStream() {
        return new BatchContainer(null, true);
    }
    
    public static BatchContainer of(Page page) {
        return new BatchContainer(page, false);
    }
}
