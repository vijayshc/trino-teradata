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
