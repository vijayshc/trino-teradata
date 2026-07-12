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
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Section 7: NULL Handling Tests")
public class NullHandlingTest extends BaseTdExportTest {

    @Test
    @DisplayName("7.1 COUNT all rows with NULLs")
    public void test7_1() {
        assertQuery("SELECT COUNT(*) FROM test_null_handling", "6");
    }

    @Test
    @DisplayName("7.2 COUNT non-null integers")
    public void test7_2() {
        assertQuery("SELECT COUNT(col_int) FROM test_null_handling", "4");
    }

    @Test
    @DisplayName("7.3 COUNT non-null decimals")
    public void test7_3() {
        assertQuery("SELECT COUNT(col_decimal) FROM test_null_handling", "4");
    }

    @Test
    @DisplayName("7.4 COUNT non-null varchars")
    public void test7_4() {
        assertQuery("SELECT COUNT(col_varchar) FROM test_null_handling", "4");
    }

    @Test
    @DisplayName("7.5 COUNT non-null dates")
    public void test7_5() {
        assertQuery("SELECT COUNT(col_date) FROM test_null_handling", "4");
    }

    @Test
    @DisplayName("7.6 NULL filter returns empty for NULL int")
    public void test7_6() {
        List<String> results = getQueryResult("SELECT col_int FROM test_null_handling WHERE col_int IS NULL");
        // JDBC returns null for actually null values in the list
        assertThat(results).allMatch(s -> s == null || s.isEmpty() || s.equalsIgnoreCase("null"));
    }
}
