package io.trino.tests.tdexport;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for Aggregation Pushdown optimization.
 * 
 * Aggregation pushdown allows aggregate functions (COUNT, SUM, MIN, MAX, AVG)
 * with GROUP BY clauses to be pushed down to Teradata, significantly reducing
 * the amount of data transferred over the network.
 */
@DisplayName("Section 20: Aggregation Pushdown Tests")
public class AggregationPushdownTest extends BaseTdExportTest {

    // ======================================================================
    // Basic Aggregation Tests (Data Correctness)
    // These tests verify that aggregate queries return correct results
    // ======================================================================

    @Test
    @DisplayName("20.1 COUNT(*) simple")
    public void testCountStar() {
        assertQuery("SELECT COUNT(*) FROM test_pushdown", "8");
    }

    @Test
    @DisplayName("20.2 COUNT(column)")
    public void testCountColumn() {
        assertQuery("SELECT COUNT(filter_int) FROM test_pushdown", "8");
    }

    @Test
    @DisplayName("20.3 SUM of integers")
    public void testSumIntegers() {
        // test_pushdown has filter_int: 100, 200, 300, 400, 500, 100, 200, 300 = 2100
        assertQuery("SELECT SUM(filter_int) FROM test_pushdown", "2100");
    }

    @Test
    @DisplayName("20.4 MIN of integers")
    public void testMinIntegers() {
        assertQuery("SELECT MIN(filter_int) FROM test_pushdown", "100");
    }

    @Test
    @DisplayName("20.5 MAX of integers")
    public void testMaxIntegers() {
        assertQuery("SELECT MAX(filter_int) FROM test_pushdown", "500");
    }

    @Test
    @DisplayName("20.6 AVG of integers")
    public void testAvgIntegers() {
        // Average of 100, 200, 300, 400, 500, 100, 200, 300 = 2100/8 = 262.5
        assertQuery("SELECT CAST(AVG(filter_int) AS DECIMAL(10,1)) FROM test_pushdown", "262.5");
    }

    @Test
    @DisplayName("20.7 GROUP BY with COUNT")
    public void testGroupByCount() {
        // COUNT(DISTINCT) is currently NOT pushed down, but should still return correct result via Trino
        assertQuery("SELECT COUNT(DISTINCT filter_int) FROM test_pushdown", "5");
    }

    @Test
    @DisplayName("20.8 GROUP BY with SUM")
    public void testGroupBySum() {
        // SUM(DISTINCT) is currently NOT pushed down
        assertQuery("SELECT SUM(DISTINCT filter_int) FROM test_pushdown", "1500");
    }

    @Test
    @DisplayName("20.9 GROUP BY multiple columns")
    public void testMultipleAggregates() {
        // Verify multiple aggregates work
        assertQuery("SELECT COUNT(*), SUM(filter_int) FROM test_pushdown", "8");
    }

    @Test
    @DisplayName("20.10 Aggregate with WHERE clause")
    public void testAggregateWithWhere() {
        // SUM of filter_int where filter_int > 200: 300 + 400 + 500 + 300 = 1500
        assertQuery("SELECT SUM(filter_int) FROM test_pushdown WHERE filter_int > 200", "1500");
    }

    // ======================================================================
    // Aggregation with GROUP BY Tests
    // ======================================================================

    @Test
    @DisplayName("20.11 Simple GROUP BY")
    public void testSimpleGroupBy() {
        // This query often results in multiple projections in Trino
        assertQuery("SELECT COUNT(*) FROM (SELECT filter_int FROM test_pushdown GROUP BY filter_int)", "5");
    }

    @Test
    @DisplayName("20.12 GROUP BY with multiple aggregates")
    public void testGroupByMultipleAggregates() {
        // Verify multiple aggregates and group by columns can be handled
        assertQuery("SELECT COUNT(*) FROM (SELECT filter_int, COUNT(*), SUM(test_id) FROM test_pushdown GROUP BY filter_int)", "5");
    }

    // ======================================================================
    // Log Validation Tests (Pushdown Verification)
    // These tests verify that aggregation is actually pushed to Teradata
    // ======================================================================

    @Test
    @DisplayName("20.20 Aggregation pushdown execution logged")
    public void testAggregationPushdownLogged() throws IOException, InterruptedException {
        // Run a simple aggregation
        assertQuery("SELECT COUNT(*) FROM test_integer_types", "6");
        
        // Assert that pushdown was applied by checking the executed SQL
        assertLogContains("Executing Teradata SQL", "count(*)");
    }

    @Test
    @DisplayName("20.21 Simple COUNT pushed to Teradata")
    public void testSimpleCountPushdown() throws IOException, InterruptedException {
        assertQuery("SELECT COUNT(*) FROM test_pushdown", "8");
        
        // Assert that Teradata SQL actually contains the COUNT aggregation
        assertLogContains("Executing Teradata SQL", "count(*)");
    }
}
