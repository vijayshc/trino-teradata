package io.trino.tests.tdexport;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for Range Predicate Pushdown optimization.
 * 
 * This test validates that range predicates (>, <, >=, <=, BETWEEN) are properly
 * pushed down to Teradata instead of being evaluated by Trino after data retrieval.
 * 
 * Test data in test_pushdown (8 rows):
 * test_id | filter_int | filter_decimal | filter_date
 * 1       | 100        | 123.45         | 2024-01-15
 * 2       | 200        | 234.56         | 2024-02-15
 * 3       | 300        | 345.67         | 2024-03-15
 * 4       | 400        | 456.78         | 2024-04-15
 * 5       | 500        | 567.89         | 2024-05-15
 * 6       | 100        | 123.45         | 2024-01-15
 * 7       | 200        | 234.56         | 2024-02-15
 * 8       | 300        | 345.67         | 2024-03-15
 */
@DisplayName("Section 18: Range Predicate Pushdown Tests")
public class RangePredicatePushdownTest extends BaseTdExportTest {

    // ======================================================================
    // Basic Range Predicate Tests (Data Correctness)
    // ======================================================================

    @Test
    @DisplayName("18.1 Greater than filter (>)")
    public void testGreaterThan() {
        // filter_int > 300: only 400 and 500 (2 rows)
        assertQuery("SELECT COUNT(*) FROM test_pushdown WHERE filter_int > 300", "2");
    }

    @Test
    @DisplayName("18.2 Less than filter (<)")
    public void testLessThan() {
        // filter_int < 300: 100 (x2) and 200 (x2) = 4 rows
        assertQuery("SELECT COUNT(*) FROM test_pushdown WHERE filter_int < 300", "4");
    }

    @Test
    @DisplayName("18.3 Greater than or equals filter (>=)")
    public void testGreaterThanOrEquals() {
        // filter_int >= 300: 300 (x2), 400, 500 = 4 rows
        assertQuery("SELECT COUNT(*) FROM test_pushdown WHERE filter_int >= 300", "4");
    }

    @Test
    @DisplayName("18.4 Less than or equals filter (<=)")
    public void testLessThanOrEquals() {
        // filter_int <= 300: 100 (x2), 200 (x2), 300 (x2) = 6 rows
        assertQuery("SELECT COUNT(*) FROM test_pushdown WHERE filter_int <= 300", "6");
    }

    @Test
    @DisplayName("18.5 BETWEEN filter (inclusive range)")
    public void testBetween() {
        // filter_int BETWEEN 200 AND 400: 200 (x2), 300 (x2), 400 = 5 rows
        assertQuery("SELECT COUNT(*) FROM test_pushdown WHERE filter_int BETWEEN 200 AND 400", "5");
    }

    @Test
    @DisplayName("18.6 Combined range filter (> AND <)")
    public void testCombinedRange() {
        // filter_int > 100 AND < 500: 200 (x2), 300 (x2), 400 = 5 rows
        assertQuery("SELECT COUNT(*) FROM test_pushdown WHERE filter_int > 100 AND filter_int < 500", "5");
    }

    @Test
    @DisplayName("18.7 Date range filter")
    public void testDateRange() {
        // filter_date >= 2024-03-01: 3-15 (x2), 4-15, 5-15 = 4 rows
        assertQuery("SELECT COUNT(*) FROM test_pushdown WHERE filter_date >= DATE '2024-03-01'", "4");
    }

    @Test
    @DisplayName("18.8 Decimal range filter")
    public void testDecimalRange() {
        // filter_decimal > 300.00: 345.67 (x2), 456.78, 567.89 = 4 rows
        assertQuery("SELECT COUNT(*) FROM test_pushdown WHERE filter_decimal > 300.00", "4");
    }

    // ======================================================================
    // Log Validation Tests (Pushdown Verification)
    // ======================================================================

    @Test
    @DisplayName("18.10 Range filter pushed to Teradata (> operator)")
    public void testRangeFilterPushedGreaterThan() throws IOException, InterruptedException {
        assertQuery("SELECT COUNT(*) FROM test_pushdown WHERE filter_int > 350", "2");
        assertLogContains("Executing Teradata SQL", "filter_int", "> ?");
    }

    @Test
    @DisplayName("18.11 Range filter pushed to Teradata (< operator)")
    public void testRangeFilterPushedLessThan() throws IOException, InterruptedException {
        assertQuery("SELECT COUNT(*) FROM test_pushdown WHERE filter_int < 250", "4");
        assertLogContains("Executing Teradata SQL", "filter_int", "< ?");
    }

    @Test
    @DisplayName("18.12 Range filter pushed to Teradata (>= operator)")
    public void testRangeFilterPushedGreaterThanOrEquals() throws IOException, InterruptedException {
        // test_integer_types has col_integer values: 0, 1, -1, max, min, normal
        assertQuery("SELECT COUNT(*) FROM test_integer_types WHERE col_integer >= 0", "4");
        assertLogContains("Executing Teradata SQL", "col_integer", ">= ?");
    }

    @Test
    @DisplayName("18.13 Range filter pushed to Teradata (<= operator)")
    public void testRangeFilterPushedLessThanOrEquals() throws IOException, InterruptedException {
        // col_integer <= 0: 0, -1, min = 3 rows
        assertQuery("SELECT COUNT(*) FROM test_integer_types WHERE col_integer <= 0", "3");
        assertLogContains("Executing Teradata SQL", "col_integer", "<= ?");
    }

    @Test
    @DisplayName("18.14 BETWEEN pushed as AND condition")
    public void testBetweenPushed() throws IOException, InterruptedException {
        // BETWEEN should be converted to >= AND <=
        assertQuery("SELECT COUNT(*) FROM test_pushdown WHERE filter_int BETWEEN 150 AND 350", "4");
        // Should see >= 150 (or > 149) and <= 350 (or < 351) in the log
        // Note: Column names are quoted in the generated SQL, e.g. "filter_int" >=
        boolean found = checkLogFor("Executing Teradata SQL", "filter_int", ">=") ||
                       checkLogFor("Executing Teradata SQL", "filter_int", ">");
        assertThat(found).as("Range predicate not found in Teradata SQL").isTrue();
    }
}
