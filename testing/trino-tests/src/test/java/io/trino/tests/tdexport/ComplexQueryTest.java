package io.trino.tests.tdexport;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Section 9 & 10: Complex Query Tests (Joins & Aggregations)")
public class ComplexQueryTest extends BaseTdExportTest {

    // Section 9: JOIN Operations
    @Test @DisplayName("9.1 Join dim table count") public void test9_1() { assertQuery("SELECT COUNT(*) FROM test_join_dim", "5"); }
    @Test @DisplayName("9.2 Join fact table count") public void test9_2() { assertQuery("SELECT COUNT(*) FROM test_join_fact", "8"); }
    @Test @DisplayName("9.3 Simple JOIN count") public void test9_3() { assertQuery("SELECT COUNT(*) FROM test_join_fact f JOIN test_join_dim d ON f.dim_id = d.dim_id", "8"); }
    @Test @DisplayName("9.4 JOIN with filter on dim") public void test9_4() { assertQuery("SELECT COUNT(*) FROM test_join_fact f JOIN test_join_dim d ON f.dim_id = d.dim_id WHERE d.dim_category = 'Type1'", "5"); }
    @Test @DisplayName("9.5 JOIN with filter on specific dim") public void test9_5() { assertQuery("SELECT COUNT(*) FROM test_join_fact f JOIN test_join_dim d ON f.dim_id = d.dim_id WHERE d.dim_name = 'Category A'", "3"); }
    @Test @DisplayName("9.6 JOIN SUM aggregation") public void test9_6() { assertQuery("SELECT CAST(SUM(f.fact_value) AS VARCHAR) FROM test_join_fact f JOIN test_join_dim d ON f.dim_id = d.dim_id WHERE d.dim_category = 'Type2'", "550.50"); }

    // Section 10: Aggregations
    @Test @DisplayName("10.1 SUM of integers") public void test10_1() { assertQuery("SELECT SUM(col_integer) FROM test_integer_types WHERE test_id IN (1,2,3)", "0"); }
    @Test @DisplayName("10.2 AVG of decimals") public void test10_2() { assertQuery("SELECT CAST(AVG(col_dec_5_2) AS DECIMAL(10,2)) FROM test_decimal_types WHERE test_id IN (4,5,6)", "222.22"); }
    @Test @DisplayName("10.3 MIN of integers") public void test10_3() { assertQuery("SELECT MIN(col_integer) FROM test_integer_types", "-2147483648"); }
    @Test @DisplayName("10.4 MAX of integers") public void test10_4() { assertQuery("SELECT MAX(col_integer) FROM test_integer_types", "2147483647"); }
    @Test @DisplayName("10.5 GROUP BY count") public void test10_5() { assertQuery("SELECT COUNT(*) FROM test_join_dim GROUP BY dim_category ORDER BY COUNT(*) DESC LIMIT 1", "2"); }
}
