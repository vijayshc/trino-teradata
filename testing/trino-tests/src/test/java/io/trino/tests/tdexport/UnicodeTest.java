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
