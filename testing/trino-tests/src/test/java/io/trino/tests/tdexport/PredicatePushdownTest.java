package io.trino.tests.tdexport;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Section 11 & 12 & 17: Predicate Pushdown Tests")
public class PredicatePushdownTest extends BaseTdExportTest {

    // Section 11: LIMIT Pushdown
    @Test @DisplayName("11.1 LIMIT 1") public void test11_1() { assertQuery("SELECT COUNT(*) FROM (SELECT * FROM test_integer_types LIMIT 1)", "1"); }
    @Test @DisplayName("11.2 LIMIT 3") public void test11_3() { assertQuery("SELECT COUNT(*) FROM (SELECT * FROM test_integer_types LIMIT 3)", "3"); }

    // Section 12: Predicate Pushdown
    @Test @DisplayName("12.1 Equality filter") public void test12_1() { assertQuery("SELECT col_byteint FROM test_integer_types WHERE test_id = 6", "42"); }
    @Test @DisplayName("12.2 IN filter") public void test12_2() { assertQuery("SELECT COUNT(*) FROM test_integer_types WHERE test_id IN (1, 2, 3)", "3"); }
    @Test @DisplayName("12.3 Range filter") public void test12_3() { assertQuery("SELECT COUNT(*) FROM test_edge_cases WHERE col_dec > 0", "2"); }
    @Test @DisplayName("12.4 String equality filter") public void test12_4() { assertQuery("SELECT test_id FROM test_char_types WHERE col_varchar_50 = 'Hello World'", "1"); }

    // Section 17: Typed Filter Tests
    @Test @DisplayName("17.1 INTEGER filter") public void test17_1() { assertQuery("SELECT test_id FROM test_pushdown WHERE filter_int = 200 ORDER BY test_id LIMIT 1", "2"); }
    @Test @DisplayName("17.2 BIGINT filter") public void test17_2() { assertQuery("SELECT test_id FROM test_pushdown WHERE filter_bigint = 3000000000000 ORDER BY test_id LIMIT 1", "3"); }
    @Test @DisplayName("17.3 DECIMAL filter") public void test17_3() { assertQuery("SELECT test_id FROM test_pushdown WHERE filter_decimal = 456.78 LIMIT 1", "4"); }
    @Test @DisplayName("17.4 VARCHAR filter") public void test17_4() { assertQuery("SELECT test_id FROM test_pushdown WHERE filter_varchar = 'epsilon' LIMIT 1", "5"); }
    @Test @DisplayName("17.5 DATE filter") public void test17_5() { assertQuery("SELECT test_id FROM test_pushdown WHERE filter_date = DATE '2024-04-15' LIMIT 1", "4"); }
    @Test @DisplayName("17.6 FLOAT filter") public void test17_6() { assertQuery("SELECT test_id FROM test_float_types WHERE col_float > 3.14 ORDER BY test_id LIMIT 1", "4"); }
    @Test @DisplayName("17.7 SMALLINT filter") public void test17_7() { assertQuery("SELECT test_id FROM test_integer_types WHERE col_smallint = 32767", "4"); }
    @Test @DisplayName("17.8 IN filter with integers") public void test17_8() { assertQuery("SELECT COUNT(*) FROM test_pushdown WHERE filter_int IN (100, 200)", "4"); }
    @Test @DisplayName("17.9 Range filter") public void test17_9() { assertQuery("SELECT COUNT(*) FROM test_pushdown WHERE filter_int > 200 AND filter_int < 500", "3"); }
}
