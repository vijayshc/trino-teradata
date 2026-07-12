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
