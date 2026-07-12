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
package io.trino.tests.tdexport;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Section 23: system.query Log Validation")
public class SystemQueryLogValidationTest extends BaseTdExportTest {

    @Test
    @DisplayName("23.1 system.query execution is wrapped with ExportToTrino")
    public void testSystemQueryWrappedByExportUdf() {
        assertQuery(
                "SELECT COUNT(*) FROM TABLE(tdexport.system.query(query => 'SELECT test_id FROM test_pushdown WHERE filter_int = 100'))",
                "2");

        assertLogContains("system.query analyzed successfully");
        assertLogContains("Executing Teradata SQL", "ExportToTrino", "test_pushdown");
    }
}