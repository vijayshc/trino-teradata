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

@DisplayName("Section 4: Floating Point Data Type Tests")
public class FloatingPointDataTypeTest extends BaseTdExportTest {

    @Test
    @DisplayName("4.1 FLOAT zero")
    public void test4_1() {
        assertQuery("SELECT col_float FROM test_float_types WHERE test_id = 1", "0.0");
    }

    @Test
    @DisplayName("4.2 FLOAT negative")
    public void test4_2() {
        assertQuery("SELECT col_float FROM test_float_types WHERE test_id = 3", "-1.0");
    }

    @Test
    @DisplayName("4.3 FLOAT Pi approximation")
    public void test4_3() {
        List<String> results = getQueryResult("SELECT ROUND(col_float, 5) FROM test_float_types WHERE test_id = 4");
        assertThat(results).isNotEmpty();
        assertThat(results.get(0)).contains("3.14159");
    }
}
