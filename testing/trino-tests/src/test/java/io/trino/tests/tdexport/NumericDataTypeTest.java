package io.trino.tests.tdexport;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Section 2 & 3 & 8: Numeric Data Type Tests")
public class NumericDataTypeTest extends BaseTdExportTest {

    // Section 2: Integer Types
    @Test @DisplayName("2.1 BYTEINT zero") public void test2_1() { assertQuery("SELECT col_byteint FROM test_integer_types WHERE test_id = 1", "0"); }
    @Test @DisplayName("2.2 BYTEINT positive max") public void test2_2() { assertQuery("SELECT col_byteint FROM test_integer_types WHERE test_id = 4", "127"); }
    @Test @DisplayName("2.3 BYTEINT negative max") public void test2_3() { assertQuery("SELECT col_byteint FROM test_integer_types WHERE test_id = 5", "-128"); }
    @Test @DisplayName("2.4 SMALLINT max") public void test2_4() { assertQuery("SELECT col_smallint FROM test_integer_types WHERE test_id = 4", "32767"); }
    @Test @DisplayName("2.5 SMALLINT min") public void test2_5() { assertQuery("SELECT col_smallint FROM test_integer_types WHERE test_id = 5", "-32768"); }
    @Test @DisplayName("2.6 INTEGER max") public void test2_6() { assertQuery("SELECT col_integer FROM test_integer_types WHERE test_id = 4", "2147483647"); }
    @Test @DisplayName("2.7 INTEGER min") public void test2_7() { assertQuery("SELECT col_integer FROM test_integer_types WHERE test_id = 5", "-2147483648"); }
    @Test @DisplayName("2.8 BIGINT max") public void test2_8() { assertQuery("SELECT col_bigint FROM test_integer_types WHERE test_id = 4", "9223372036854775807"); }
    @Test @DisplayName("2.9 BIGINT min") public void test2_9() { assertQuery("SELECT col_bigint FROM test_integer_types WHERE test_id = 5", "-9223372036854775808"); }
    @Test @DisplayName("2.10 BIGINT large value") public void test2_10() { assertQuery("SELECT col_bigint FROM test_integer_types WHERE test_id = 6", "9876543210123"); }

    // Section 3: Decimal Types
    @Test @DisplayName("3.1 DECIMAL(5,2) zero") public void test3_1() { assertQuery("SELECT col_dec_5_2 FROM test_decimal_types WHERE test_id = 1", "0.00"); }
    @Test @DisplayName("3.2 DECIMAL(5,2) positive") public void test3_2() { assertQuery("SELECT col_dec_5_2 FROM test_decimal_types WHERE test_id = 5", "123.45"); }
    @Test @DisplayName("3.3 DECIMAL(5,2) negative") public void test3_3() { assertQuery("SELECT col_dec_5_2 FROM test_decimal_types WHERE test_id = 6", "-456.78"); }
    @Test @DisplayName("3.4 DECIMAL(10,4) value") public void test3_4() { assertQuery("SELECT col_dec_10_4 FROM test_decimal_types WHERE test_id = 5", "1234.5678"); }
    @Test @DisplayName("3.5 DECIMAL(18,6) value") public void test3_5() { assertQuery("SELECT col_dec_18_6 FROM test_decimal_types WHERE test_id = 5", "123456.789012"); }

    // Section 8: Edge Cases Decimal
    @Test @DisplayName("8.1 Small positive decimal") public void test8_1() { assertQuery("SELECT col_dec FROM test_edge_cases WHERE test_id = 1", "0.01"); }
    @Test @DisplayName("8.2 Zero decimal") public void test8_2() { assertQuery("SELECT col_dec FROM test_edge_cases WHERE test_id = 2", "0.00"); }
    @Test @DisplayName("8.3 Small negative decimal") public void test8_3() { assertQuery("SELECT col_dec FROM test_edge_cases WHERE test_id = 3", "-0.01"); }
    @Test @DisplayName("8.4 Max precision decimal") public void test8_4() { assertQuery("SELECT col_dec FROM test_edge_cases WHERE test_id = 4", "99999.99"); }
    @Test @DisplayName("8.5 Min precision decimal") public void test8_5() { assertQuery("SELECT col_dec FROM test_edge_cases WHERE test_id = 5", "-99999.99"); }
}
