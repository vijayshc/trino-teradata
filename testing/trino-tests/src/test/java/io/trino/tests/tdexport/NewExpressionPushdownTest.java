package io.trino.tests.tdexport;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for newly added expression rewriter rules and aggregate functions.
 * 
 * Covers:
 * - COUNT(DISTINCT) pushdown (ImplementCountDistinct)
 * - IN clause pushdown
 * - BETWEEN clause pushdown
 */
@DisplayName("Section 22: New Expression Pushdown Tests")
public class NewExpressionPushdownTest extends BaseTdExportTest {

    // ======================================================================
    // COUNT(DISTINCT) Tests - ImplementCountDistinct
    // ======================================================================

    @Test
    @DisplayName("22.1 COUNT(DISTINCT column) basic")
    public void testCountDistinctBasic() {
        // test_pushdown has filter_int: 100, 200, 300, 400, 500, 100, 200, 300
        // Distinct values: 100, 200, 300, 400, 500 = 5
        assertQuery("SELECT COUNT(DISTINCT filter_int) FROM test_pushdown", "5");
    }

    @Test
    @DisplayName("22.2 COUNT(DISTINCT column) with WHERE")
    public void testCountDistinctWithWhere() {
        // Filter to only include filter_int > 200
        // Values matching: 300, 400, 500, 300 -> Distinct: 300, 400, 500 = 3
        assertQuery("SELECT COUNT(DISTINCT filter_int) FROM test_pushdown WHERE filter_int > 200", "3");
    }

    @Test
    @DisplayName("22.3 COUNT(DISTINCT varchar)")
    public void testCountDistinctVarchar() {
        // test_pushdown has filter_varchar: alpha, beta, gamma, delta, epsilon, alpha, beta, gamma
        // Distinct values: alpha, beta, gamma, delta, epsilon = 5
        assertQuery("SELECT COUNT(DISTINCT filter_varchar) FROM test_pushdown", "5");
    }

    @Test
    @DisplayName("22.4 COUNT(DISTINCT) with GROUP BY")
    public void testCountDistinctWithGroupBy() {
        // Count distinct filter_int values per filter_varchar group
        List<String> result = getQueryResult(
            "SELECT filter_varchar, COUNT(DISTINCT filter_int) as distinct_count " +
            "FROM test_pushdown GROUP BY filter_varchar ORDER BY filter_varchar");
        assertThat(result).isNotEmpty();
        log.info("22.4 COUNT(DISTINCT) with GROUP BY: " + result.size() + " groups");
    }

    @Test
    @DisplayName("22.5 COUNT(DISTINCT) with multiple columns")
    public void testCountDistinctMultipleColumns() {
        // Test COUNT(DISTINCT) with different data types
        String sql = "SELECT COUNT(DISTINCT filter_int), COUNT(DISTINCT filter_varchar) FROM test_pushdown";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("22.5 COUNT(DISTINCT) multiple columns: " + result.get(0));
    }

    // ======================================================================
    // IN Clause Tests
    // ======================================================================

    @Test
    @DisplayName("22.6 IN clause with integers")
    public void testInClauseIntegers() {
        // Count rows where filter_int is in (100, 200)
        // filter_int values: 100, 200, 300, 400, 500, 100, 200, 300
        // Matches: 100, 200, 100, 200 = 4 rows
        assertQuery("SELECT COUNT(*) FROM test_pushdown WHERE filter_int IN (100, 200)", "4");
    }

    @Test
    @DisplayName("22.7 IN clause with varchar")
    public void testInClauseVarchar() {
        // Count rows where filter_varchar is in ('alpha', 'beta')
        assertQuery("SELECT COUNT(*) FROM test_pushdown WHERE filter_varchar IN ('alpha', 'beta')", "4");
    }

    @Test
    @DisplayName("22.8 NOT IN clause")
    public void testNotInClause() {
        // Count rows where filter_int is NOT in (100, 200)
        // Matches: 300, 400, 500, 300 = 4 rows
        assertQuery("SELECT COUNT(*) FROM test_pushdown WHERE filter_int NOT IN (100, 200)", "4");
    }

    @Test
    @DisplayName("22.9 IN clause with subquery")
    public void testInClauseWithSubquery() {
        // Use IN with a subquery - this may or may not push down fully
        String sql = "SELECT COUNT(*) FROM test_pushdown " +
                     "WHERE filter_int IN (SELECT DISTINCT filter_int FROM test_pushdown WHERE filter_int <= 200)";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("22.9 IN with subquery result: " + result.get(0));
    }

    @Test
    @DisplayName("22.10 IN clause with large list")
    public void testInClauseLargeList() {
        // Test IN with multiple values to verify correct handling
        assertQuery("SELECT COUNT(*) FROM test_pushdown WHERE filter_int IN (100, 200, 300, 400, 500)", "8");
    }

    // ======================================================================
    // BETWEEN Clause Tests
    // ======================================================================

    @Test
    @DisplayName("22.11 BETWEEN with integers")
    public void testBetweenIntegers() {
        // Count rows where filter_int BETWEEN 200 AND 400
        // filter_int values: 100, 200, 300, 400, 500, 100, 200, 300
        // Matches: 200, 300, 400, 200, 300 = 5 rows
        assertQuery("SELECT COUNT(*) FROM test_pushdown WHERE filter_int BETWEEN 200 AND 400", "5");
    }

    @Test
    @DisplayName("22.12 BETWEEN with dates")
    public void testBetweenDates() {
        // Test BETWEEN with date values
        String sql = "SELECT COUNT(*) FROM test_pushdown " +
                     "WHERE filter_date BETWEEN DATE '2024-01-01' AND DATE '2024-06-30'";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("22.12 BETWEEN dates result: " + result.get(0));
    }

    @Test
    @DisplayName("22.13 NOT BETWEEN")
    public void testNotBetween() {
        // Count rows where filter_int NOT BETWEEN 200 AND 400
        // Matches: 100, 500, 100 = 3 rows
        assertQuery("SELECT COUNT(*) FROM test_pushdown WHERE filter_int NOT BETWEEN 200 AND 400", "3");
    }

    @Test
    @DisplayName("22.14 BETWEEN combined with other predicates")
    public void testBetweenCombined() {
        // Combine BETWEEN with other filters
        String sql = "SELECT COUNT(*) FROM test_pushdown " +
                     "WHERE filter_int BETWEEN 200 AND 400 AND filter_varchar IN ('alpha', 'beta', 'gamma')";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("22.14 BETWEEN combined result: " + result.get(0));
    }

    @Test
    @DisplayName("22.15 BETWEEN with decimals")
    public void testBetweenDecimals() {
        // Test BETWEEN with decimal values
        String sql = "SELECT COUNT(*) FROM test_pushdown " +
                     "WHERE filter_decimal BETWEEN 200.00 AND 400.00";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("22.15 BETWEEN decimals result: " + result.get(0));
    }

    // ======================================================================
    // Combined Expression Tests
    // ======================================================================

    @Test
    @DisplayName("22.16 COUNT(DISTINCT) with IN clause filter")
    public void testCountDistinctWithInFilter() {
        // Combine COUNT(DISTINCT) with IN filter
        String sql = "SELECT COUNT(DISTINCT filter_varchar) FROM test_pushdown " +
                     "WHERE filter_int IN (100, 200, 300)";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("22.16 COUNT(DISTINCT) with IN: " + result.get(0));
    }

    @Test
    @DisplayName("22.17 COUNT(DISTINCT) with BETWEEN filter")
    public void testCountDistinctWithBetweenFilter() {
        // Combine COUNT(DISTINCT) with BETWEEN filter
        String sql = "SELECT COUNT(DISTINCT filter_varchar) FROM test_pushdown " +
                     "WHERE filter_int BETWEEN 200 AND 400";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("22.17 COUNT(DISTINCT) with BETWEEN: " + result.get(0));
    }

    @Test
    @DisplayName("22.18 Multiple IN clauses")
    public void testMultipleInClauses() {
        // Test multiple IN clauses combined
        String sql = "SELECT COUNT(*) FROM test_pushdown " +
                     "WHERE filter_int IN (100, 200, 300) AND filter_varchar IN ('alpha', 'beta')";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("22.18 Multiple IN clauses: " + result.get(0));
    }

    @Test
    @DisplayName("22.19 IN and BETWEEN combined")
    public void testInAndBetweenCombined() {
        String sql = "SELECT test_id, filter_varchar, filter_int FROM test_pushdown " +
                     "WHERE filter_varchar IN ('alpha', 'beta', 'gamma') " +
                     "AND filter_int BETWEEN 100 AND 300 " +
                     "ORDER BY test_id";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("22.19 IN and BETWEEN combined: " + result.size() + " rows");
    }

    @Test
    @DisplayName("22.20 Complex expression with all new features")
    public void testComplexExpressionAllFeatures() {
        // Complex query using COUNT(DISTINCT), IN, and BETWEEN together
        String sql = "SELECT filter_varchar, " +
                     "COUNT(DISTINCT filter_int) as distinct_ints, " +
                     "COUNT(*) as total_count " +
                     "FROM test_pushdown " +
                     "WHERE filter_int BETWEEN 100 AND 400 " +
                     "AND filter_varchar IN ('alpha', 'beta', 'gamma', 'delta') " +
                     "GROUP BY filter_varchar " +
                     "ORDER BY filter_varchar";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("22.20 Complex expression: " + result.size() + " groups");
    }
}
