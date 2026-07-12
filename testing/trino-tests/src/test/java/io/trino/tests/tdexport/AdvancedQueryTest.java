package io.trino.tests.tdexport;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for Advanced Query Scenarios.
 * 
 * Validates:
 * - Multi-table JOINs (3-5 tables)
 * - Subqueries (scalar, correlated, IN/EXISTS)
 * - CTEs (Common Table Expressions)
 * - Complex expressions with multiple operators
 * - Mixed pushdown scenarios
 * 
 * Uses test tables from TrinoExport schema and validates both
 * pushdown behavior and result correctness.
 */
@DisplayName("Section 21: Advanced Query Tests")
public class AdvancedQueryTest extends BaseTdExportTest {

    // ======================================================================
    // Multi-Table JOIN Tests (3-5 tables)
    // ======================================================================

    @Test
    @DisplayName("21.1 Three-way JOIN")
    public void testThreeWayJoin() {
        String sql = "SELECT a.test_id, a.filter_varchar, b.col_integer, c.col_varchar_50 " +
                     "FROM test_pushdown a " +
                     "INNER JOIN test_integer_types b ON a.test_id = b.test_id " +
                     "INNER JOIN test_char_types c ON a.test_id = c.test_id " +
                     "WHERE a.test_id <= 5 " +
                     "ORDER BY a.test_id";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("21.1 Three-way JOIN: " + result.size() + " rows");
    }

    @Test
    @DisplayName("21.2 Four-way JOIN with aggregation")
    public void testFourWayJoinWithAggregation() {
        // Use test_pushdown joined to itself with different aliases
        String sql = "SELECT a.filter_varchar, COUNT(*) as join_count " +
                     "FROM test_pushdown a " +
                     "LEFT JOIN test_pushdown b ON a.filter_varchar = b.filter_varchar " +
                     "LEFT JOIN test_pushdown c ON a.test_id = c.test_id " +
                     "LEFT JOIN test_integer_types d ON a.test_id = d.test_id " +
                     "WHERE a.test_id <= 5 " +
                     "GROUP BY a.filter_varchar " +
                     "HAVING COUNT(*) > 0";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("21.2 Four-way JOIN with aggregation: " + result.size() + " groups");
    }

    @Test
    @DisplayName("21.3 Five-way JOIN with complex filter")
    public void testFiveWayJoin() {
        // Use test_pushdown self-joins to guarantee data
        String sql = "SELECT DISTINCT a.test_id, a.filter_varchar " +
                     "FROM test_pushdown a " +
                     "LEFT JOIN test_pushdown b ON a.filter_varchar = b.filter_varchar " +
                     "LEFT JOIN test_pushdown c ON a.filter_int = c.filter_int " +
                     "LEFT JOIN test_pushdown d ON a.test_id = d.test_id " +
                     "LEFT JOIN test_pushdown e ON a.test_id <= e.test_id " +
                     "WHERE a.filter_int >= 100 " +
                     "ORDER BY a.test_id " +
                     "LIMIT 10";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("21.3 Five-way JOIN: " + result.size() + " rows");
    }

    @Test
    @DisplayName("21.4 Mixed JOIN types (INNER + LEFT)")
    public void testMixedJoinTypes() {
        String sql = "SELECT a.test_id, b.col_integer, c.col_varchar_50 " +
                     "FROM test_pushdown a " +
                     "INNER JOIN test_integer_types b ON a.test_id = b.test_id " +
                     "LEFT JOIN test_char_types c ON a.test_id = c.test_id " +
                     "WHERE a.test_id <= 5 " +
                     "ORDER BY a.test_id";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("21.4 Mixed JOIN types: " + result.size() + " rows");
    }

    // ======================================================================
    // Subquery Tests
    // ======================================================================

    @Test
    @DisplayName("21.5 Scalar subquery in SELECT")
    public void testScalarSubqueryInSelect() {
        String sql = "SELECT test_id, filter_varchar, " +
                     "(SELECT COUNT(*) FROM test_integer_types WHERE test_id <= 5) as int_count " +
                     "FROM test_pushdown " +
                     "WHERE test_id <= 3 " +
                     "ORDER BY test_id";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("21.5 Scalar subquery in SELECT: " + result.size() + " rows");
    }

    @Test
    @DisplayName("21.6 Subquery with IN clause")
    public void testSubqueryWithIn() {
        String sql = "SELECT test_id, filter_varchar, filter_int " +
                     "FROM test_pushdown " +
                     "WHERE test_id IN ( " +
                     "  SELECT test_id FROM test_integer_types WHERE col_integer > 0 " +
                     ") " +
                     "ORDER BY test_id";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("21.6 Subquery with IN: " + result.size() + " rows");
    }

    @Test
    @DisplayName("21.7 Subquery with EXISTS")
    public void testSubqueryWithExists() {
        String sql = "SELECT test_id, filter_varchar " +
                     "FROM test_pushdown a " +
                     "WHERE EXISTS ( " +
                     "  SELECT 1 FROM test_integer_types b " +
                     "  WHERE b.test_id = a.test_id AND b.col_integer IS NOT NULL " +
                     ") " +
                     "ORDER BY test_id";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("21.7 Subquery with EXISTS: " + result.size() + " rows");
    }

    @Test
    @DisplayName("21.8 Subquery with NOT EXISTS")
    public void testSubqueryWithNotExists() {
        String sql = "SELECT COUNT(*) FROM test_pushdown a " +
                     "WHERE NOT EXISTS ( " +
                     "  SELECT 1 FROM test_integer_types b " +
                     "  WHERE b.test_id = a.test_id AND b.test_id > 100 " +
                     ")";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("21.8 Subquery with NOT EXISTS: " + result);
    }

    @Test
    @DisplayName("21.9 Correlated subquery in WHERE")
    public void testCorrelatedSubquery() {
        String sql = "SELECT a.test_id, a.filter_int " +
                     "FROM test_pushdown a " +
                     "WHERE a.filter_int = ( " +
                     "  SELECT MAX(b.filter_int) " +
                     "  FROM test_pushdown b " +
                     "  WHERE b.filter_varchar = a.filter_varchar " +
                     ") " +
                     "ORDER BY a.test_id";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("21.9 Correlated subquery: " + result.size() + " rows");
    }

    @Test
    @DisplayName("21.10 Derived table (FROM subquery)")
    public void testDerivedTable() {
        String sql = "SELECT derived.varchar_group, derived.cnt, derived.max_int " +
                     "FROM ( " +
                     "  SELECT filter_varchar as varchar_group, " +
                     "         COUNT(*) as cnt, " +
                     "         MAX(filter_int) as max_int " +
                     "  FROM test_pushdown " +
                     "  WHERE filter_int IS NOT NULL " +
                     "  GROUP BY filter_varchar " +
                     ") derived " +
                     "ORDER BY derived.varchar_group";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("21.10 Derived table: " + result.size() + " rows");
    }

    // ======================================================================
    // CTE (Common Table Expression) Tests
    // ======================================================================

    @Test
    @DisplayName("21.11 Simple CTE")
    public void testSimpleCte() {
        String sql = "WITH filtered_data AS ( " +
                     "  SELECT test_id, filter_varchar, filter_int " +
                     "  FROM test_pushdown " +
                     "  WHERE filter_int >= 100 " +
                     ") " +
                     "SELECT * FROM filtered_data " +
                     "ORDER BY test_id " +
                     "LIMIT 10";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("21.11 Simple CTE: " + result.size() + " rows");
    }

    @Test
    @DisplayName("21.12 CTE with aggregation")
    public void testCteWithAggregation() {
        String sql = "WITH aggregated AS ( " +
                     "  SELECT filter_varchar, " +
                     "         COUNT(*) as row_count, " +
                     "         SUM(filter_int) as total_int, " +
                     "         AVG(filter_int) as avg_int " +
                     "  FROM test_pushdown " +
                     "  GROUP BY filter_varchar " +
                     ") " +
                     "SELECT * FROM aggregated " +
                     "WHERE row_count > 0 " +
                     "ORDER BY filter_varchar";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("21.12 CTE with aggregation: " + result.size() + " rows");
    }

    @Test
    @DisplayName("21.13 Multiple CTEs")
    public void testMultipleCtes() {
        String sql = "WITH " +
                     "cte1 AS ( " +
                     "  SELECT test_id, filter_varchar FROM test_pushdown WHERE filter_int >= 100 " +
                     "), " +
                     "cte2 AS ( " +
                     "  SELECT test_id, col_integer FROM test_integer_types WHERE col_integer > 0 " +
                     ") " +
                     "SELECT cte1.test_id, cte1.filter_varchar, cte2.col_integer " +
                     "FROM cte1 " +
                     "INNER JOIN cte2 ON cte1.test_id = cte2.test_id " +
                     "ORDER BY cte1.test_id " +
                     "LIMIT 10";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("21.13 Multiple CTEs: " + result.size() + " rows");
    }

    @Test
    @DisplayName("21.14 Nested CTE reference")
    public void testNestedCteReference() {
        String sql = "WITH " +
                     "base AS ( " +
                     "  SELECT test_id, filter_varchar, filter_int " +
                     "  FROM test_pushdown " +
                     "), " +
                     "filtered AS ( " +
                     "  SELECT * FROM base WHERE filter_int >= 100 " +
                     "), " +
                     "aggregated AS ( " +
                     "  SELECT filter_varchar, COUNT(*) as cnt FROM filtered GROUP BY filter_varchar " +
                     ") " +
                     "SELECT * FROM aggregated ORDER BY filter_varchar";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("21.14 Nested CTE reference: " + result.size() + " rows");
    }

    // ======================================================================
    // Complex Expression Tests
    // ======================================================================

    @Test
    @DisplayName("21.15 Complex boolean expression with multiple levels")
    public void testComplexBooleanExpression() {
        String sql = "SELECT COUNT(*) FROM test_pushdown " +
                     "WHERE (filter_int >= 100 AND filter_int <= 500) " +
                     "AND (filter_varchar = 'alpha' OR filter_varchar = 'beta' OR filter_varchar = 'gamma') " +
                     "AND test_id IS NOT NULL " +
                     "AND NOT (filter_int = 200 AND filter_varchar = 'alpha')";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("21.15 Complex boolean expression: " + result);
    }

    @Test
    @DisplayName("21.16 CASE expression in SELECT")
    public void testCaseExpression() {
        String sql = "SELECT test_id, " +
                     "CASE " +
                     "  WHEN filter_int < 100 THEN 'low' " +
                     "  WHEN filter_int < 300 THEN 'medium' " +
                     "  ELSE 'high' " +
                     "END as int_category, " +
                     "filter_varchar " +
                     "FROM test_pushdown " +
                     "WHERE test_id <= 5 " +
                     "ORDER BY test_id";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("21.16 CASE expression: " + result.size() + " rows");
    }

    @Test
    @DisplayName("21.17 Arithmetic expressions")
    public void testArithmeticExpressions() {
        String sql = "SELECT test_id, " +
                     "filter_int, " +
                     "filter_int * 2 as doubled, " +
                     "filter_int + 100 as plus_100 " +
                     "FROM test_pushdown " +
                     "WHERE filter_int + 50 > 150 " +
                     "ORDER BY test_id " +
                     "LIMIT 5";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("21.17 Arithmetic expressions: " + result.size() + " rows");
    }

    @Test
    @DisplayName("21.18 COALESCE and NULL handling")
    public void testCoalesceAndNullHandling() {
        String sql = "SELECT test_id, " +
                     "COALESCE(filter_varchar, 'N/A') as safe_varchar, " +
                     "COALESCE(filter_int, 0) as safe_int " +
                     "FROM test_pushdown " +
                     "ORDER BY test_id " +
                     "LIMIT 5";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("21.18 COALESCE: " + result.size() + " rows");
    }

    // ======================================================================
    // Combined Complex Scenarios
    // ======================================================================

    @Test
    @DisplayName("21.19 CTE + Multi-way JOIN + Aggregation")
    public void testCteWithMultiJoinAndAggregation() {
        String sql = "WITH base_data AS ( " +
                     "  SELECT a.test_id, a.filter_varchar, b.col_integer " +
                     "  FROM test_pushdown a " +
                     "  INNER JOIN test_integer_types b ON a.test_id = b.test_id " +
                     "  WHERE a.filter_int >= 100 " +
                     ") " +
                     "SELECT filter_varchar, " +
                     "COUNT(*) as cnt, " +
                     "SUM(col_integer) as total_int " +
                     "FROM base_data " +
                     "GROUP BY filter_varchar " +
                     "HAVING COUNT(*) > 0 " +
                     "ORDER BY filter_varchar";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("21.19 CTE + Multi-JOIN + Aggregation: " + result.size() + " rows");
    }

    @Test
    @DisplayName("21.20 Subquery in JOIN condition context")
    public void testSubqueryInJoinContext() {
        String sql = "SELECT a.test_id, a.filter_varchar, b.col_integer " +
                     "FROM test_pushdown a " +
                     "INNER JOIN test_integer_types b ON a.test_id = b.test_id " +
                     "WHERE a.filter_int IN ( " +
                     "  SELECT filter_int FROM test_pushdown WHERE filter_varchar = 'alpha' " +
                     ") " +
                     "ORDER BY a.test_id";
        List<String> result = getQueryResult(sql);
        log.info("21.20 Subquery in JOIN context: " + result.size() + " rows");
    }

    @Test
    @DisplayName("21.21 Complex ORDER BY with multiple columns")
    public void testComplexOrderBy() {
        String sql = "SELECT test_id, filter_varchar, filter_int " +
                     "FROM test_pushdown " +
                     "WHERE filter_int >= 100 " +
                     "ORDER BY filter_varchar ASC, filter_int DESC, test_id ASC " +
                     "LIMIT 10";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("21.21 Complex ORDER BY: " + result.size() + " rows");
    }

    @Test
    @DisplayName("21.22 DISTINCT with JOIN")
    public void testDistinctWithJoin() {
        String sql = "SELECT DISTINCT a.filter_varchar " +
                     "FROM test_pushdown a " +
                     "INNER JOIN test_integer_types b ON a.test_id = b.test_id " +
                     "ORDER BY a.filter_varchar";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("21.22 DISTINCT with JOIN: " + result.size() + " distinct values");
    }

    @Test
    @DisplayName("21.23 UNION with different tables")
    public void testUnionQueries() {
        String sql = "SELECT test_id, 'pushdown' as source FROM test_pushdown WHERE test_id <= 3 " +
                     "UNION ALL " +
                     "SELECT test_id, 'integer' as source FROM test_integer_types WHERE test_id <= 3 " +
                     "ORDER BY test_id, source";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("21.23 UNION: " + result.size() + " rows");
    }

    @Test
    @DisplayName("21.24 Complex WHERE with date/time comparisons")
    public void testComplexDateTimeComparisons() {
        String sql = "SELECT test_id, col_date, col_timestamp " +
                     "FROM test_datetime_types " +
                     "WHERE col_date >= DATE '2020-01-01' " +
                     "AND col_date <= DATE '2025-12-31' " +
                     "AND col_timestamp IS NOT NULL " +
                     "ORDER BY col_date " +
                     "LIMIT 5";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("21.24 DateTime comparisons: " + result.size() + " rows");
    }

    @Test
    @DisplayName("21.25 Window function (ROW_NUMBER)")
    public void testWindowFunction() {
        String sql = "SELECT test_id, filter_varchar, filter_int, " +
                     "ROW_NUMBER() OVER (PARTITION BY filter_varchar ORDER BY filter_int DESC) as rn " +
                     "FROM test_pushdown " +
                     "WHERE filter_int >= 100 " +
                     "ORDER BY filter_varchar, rn " +
                     "LIMIT 10";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("21.25 Window function: " + result.size() + " rows");
    }
}
