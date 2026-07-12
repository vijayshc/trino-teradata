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

@DisplayName("Basic Connectivity Tests")
public class BasicConnectivityTest extends BaseTdExportTest {

    @Test
    @DisplayName("1.1 Integer types table exists")
    public void testIntegerTypesExist() {
        assertQuery("SELECT COUNT(*) FROM test_integer_types", "6");
    }

    @Test
    @DisplayName("1.2 Decimal types table exists")
    public void testDecimalTypesExist() {
        assertQuery("SELECT COUNT(*) FROM test_decimal_types", "6");
    }

    @Test
    @DisplayName("1.3 Float types table exists")
    public void testFloatTypesExist() {
        assertQuery("SELECT COUNT(*) FROM test_float_types", "7");
    }

    @Test
    @DisplayName("1.4 Char types table exists")
    public void testCharTypesExist() {
        assertQuery("SELECT COUNT(*) FROM test_char_types", "5");
    }

    @Test
    @DisplayName("1.5 Datetime types table exists")
    public void testDatetimeTypesExist() {
        assertQuery("SELECT COUNT(*) FROM test_datetime_types", "5");
    }

    @Test
    @DisplayName("1.6 Null handling table exists")
    public void testNullHandlingExist() {
        assertQuery("SELECT COUNT(*) FROM test_null_handling", "6");
    }

    @Test
    @DisplayName("1.7 Edge cases table exists")
    public void testEdgeCasesExist() {
        assertQuery("SELECT COUNT(*) FROM test_edge_cases", "5");
    }
}
