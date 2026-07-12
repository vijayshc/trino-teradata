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

@DisplayName("Character Data Type Tests")
public class CharDataTypeTest extends BaseTdExportTest {

    @Test
    @DisplayName("5.1 VARCHAR basic")
    public void testVarcharBasic() {
        assertQuery("SELECT col_varchar_50 FROM test_char_types WHERE test_id = 1", "Hello World");
    }

    @Test
    @DisplayName("5.2 VARCHAR empty")
    public void testVarcharEmpty() {
        assertQuery("SELECT col_varchar_50 FROM test_char_types WHERE test_id = 2", "");
    }

    @Test
    @DisplayName("5.3 VARCHAR with special chars")
    public void testVarcharSpecial() {
        assertQuery("SELECT col_varchar_50 FROM test_char_types WHERE test_id = 3", "ABC123!@#$%");
    }

    @Test
    @DisplayName("5.4 VARCHAR with single quote")
    public void testVarcharQuote() {
        assertQuery("SELECT col_varchar_50 FROM test_char_types WHERE test_id = 4", "It's a test");
    }

    @Test
    @DisplayName("5.5 CHAR fixed length")
    public void testCharFixed() {
        assertQuery("SELECT TRIM(col_char_10) FROM test_char_types WHERE test_id = 1", "Hello");
    }
}
