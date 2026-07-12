package io.trino.tests.tdexport;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for Top-N Pushdown optimization.
 * 
 * Top-N pushdown optimizes queries with ORDER BY ... LIMIT N by pushing the
 * sorting and limiting operation to Teradata. This is more efficient than
 * fetching all rows and sorting/limiting in Trino.
 * 
 * For Top-N queries:
 * - Teradata uses: SELECT TOP N ... ORDER BY ...
 * - For plain LIMIT (no ORDER BY): Teradata uses SAMPLE N (random rows)
 */
@DisplayName("Section 19: Top-N Pushdown Tests")
public class TopNPushdownTest extends BaseTdExportTest {

    // ======================================================================
    // Basic Top-N Tests (Data Correctness)
    // ======================================================================

    @Test
    @DisplayName("19.1 ORDER BY ASC LIMIT - returns correct count")
    public void testOrderByAscLimit() {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM test_integer_types ORDER BY test_id ASC LIMIT 3)", "3");
    }

    @Test
    @DisplayName("19.2 ORDER BY DESC LIMIT - returns correct count")
    public void testOrderByDescLimit() {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM test_integer_types ORDER BY test_id DESC LIMIT 3)", "3");
    }

    @Test
    @DisplayName("19.3 ORDER BY ASC LIMIT 1 - returns minimum value")
    public void testOrderByAscLimitOne() {
        // test_integer_types has test_id 1-6, so ORDER BY ASC LIMIT 1 should return 1
        assertQuery("SELECT test_id FROM test_integer_types ORDER BY test_id ASC LIMIT 1", "1");
    }

    @Test
    @DisplayName("19.4 ORDER BY DESC LIMIT 1 - returns maximum value")
    public void testOrderByDescLimitOne() {
        // test_integer_types has test_id 1-6, so ORDER BY DESC LIMIT 1 should return 6
        assertQuery("SELECT test_id FROM test_integer_types ORDER BY test_id DESC LIMIT 1", "6");
    }

    @Test
    @DisplayName("19.5 ORDER BY with multiple columns")
    public void testOrderByMultipleColumns() throws SQLException {
        // Just verify it returns correct count - ordering by multiple columns
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM test_pushdown ORDER BY filter_int ASC, test_id DESC LIMIT 5)", "5");
    }

    @Test
    @DisplayName("19.6 ORDER BY + LIMIT + WHERE combined")
    public void testOrderByLimitWithWhere() {
        // Combine predicate pushdown with Top-N pushdown
        assertQuery("SELECT test_id FROM test_integer_types WHERE test_id > 2 ORDER BY test_id ASC LIMIT 1", "3");
    }

    @Test
    @DisplayName("19.7 Top-N with column projection")
    public void testTopNWithProjection() throws SQLException {
        // Verify that column pruning works with Top-N
        List<String> results = getMultipleResults("SELECT test_id, col_byteint FROM test_integer_types ORDER BY test_id ASC LIMIT 2");
        assertThat(results).hasSize(2);
    }

    @Test
    @DisplayName("19.8 ORDER BY on non-integer column")
    public void testOrderByVarchar() {
        // Order by varchar column
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM test_char_types ORDER BY col_varchar_50 ASC LIMIT 3)", "3");
    }

    @Test
    @DisplayName("19.9 ORDER BY on date column")
    public void testOrderByDate() {
        // Order by date column
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM test_datetime_types ORDER BY col_date ASC LIMIT 2)", "2");
    }

    @Test
    @DisplayName("19.10 Plain LIMIT (no ORDER BY) uses SAMPLE")
    public void testPlainLimitUsesSample() {
        // Without ORDER BY, LIMIT should use SAMPLE (random rows)
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM test_integer_types LIMIT 3)", "3");
    }

    // ======================================================================
    // Log Validation Tests (Pushdown Verification)
    // ======================================================================

    @Test
    @DisplayName("19.11 Top-N pushed to Teradata (ORDER BY + TOP)")
    public void testTopNPushedToTeradata() throws IOException, InterruptedException {
        assertQuery("SELECT test_id FROM test_pushdown ORDER BY test_id ASC LIMIT 3", "1");
        
        // Should see TOP N and ORDER BY in Teradata SQL
        assertLogContains("Executing Teradata SQL", "TOP 3", "ORDER BY");
    }

    @Test
    @DisplayName("19.12 Plain LIMIT uses SAMPLE (not TOP)")
    public void testPlainLimitSampleInLog() throws IOException, InterruptedException {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM test_pushdown LIMIT 4)", "4");
        
        // Plain LIMIT without ORDER BY should use SAMPLE
        assertLogContains("Executing Teradata SQL", "SAMPLE 4");
    }

    @Test
    @DisplayName("19.13 Top-N with WHERE clause pushed together")
    public void testTopNWithWherePushed() throws IOException, InterruptedException {
        assertQuery("SELECT test_id FROM test_pushdown WHERE filter_int > 200 ORDER BY test_id ASC LIMIT 2", "3");
        
        // Should have both filter and TOP/ORDER BY in SQL
        assertLogContains("Executing Teradata SQL", "filter_int", "> ?", "TOP 2", "ORDER BY");
    }

    @Test
    @DisplayName("19.14 ORDER BY DESC direction preserved")
    public void testOrderByDescPreserved() throws IOException, InterruptedException {
        assertQuery("SELECT test_id FROM test_integer_types ORDER BY test_id DESC LIMIT 1", "6");
        
        // Should see DESC in the ORDER BY clause
        assertLogContains("Executing Teradata SQL", "DESC");
    }

    // ======================================================================
    // Helper Methods
    // ======================================================================

    private List<String> getMultipleResults(String sql) throws SQLException {
        List<String> results = new ArrayList<>();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                results.add(rs.getString(1));
            }
        }
        return results;
    }
}
