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

@DisplayName("Unicode Character Tests")
public class UnicodeTest extends BaseTdExportTest {

    @Test
    @DisplayName("13.2 Chinese characters: 中文测试")
    public void testChineseChars() {
        assertQueryContains("SELECT col_unicode FROM test_unicode WHERE test_id = 1", "中文测试");
    }

    @Test
    @DisplayName("13.3 Thai characters: ทดสอบ")
    public void testThaiChars() {
        assertQueryContains("SELECT col_unicode FROM test_unicode WHERE test_id = 2", "ทดสอบ");
    }

    @Test
    @DisplayName("13.4 Mixed Unicode/ASCII")
    public void testMixedUnicode() {
        assertQueryContains("SELECT col_unicode FROM test_unicode WHERE test_id = 6", "Test 中文 Mix");
    }
}
