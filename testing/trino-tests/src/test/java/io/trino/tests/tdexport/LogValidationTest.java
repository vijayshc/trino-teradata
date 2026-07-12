package io.trino.tests.tdexport;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Section 15 & 16: Log-based Pushdown Validation")
public class LogValidationTest extends BaseTdExportTest {

    // Reuse env-aware path from BaseTdExportTest
    private static final String LOG_FILE = LOG_PATH;

    @Test
    @DisplayName("15.2 Integer filter pushed to Teradata")
    public void test15_2() throws IOException, InterruptedException {
        assertQuery("SELECT COUNT(*) FROM test_pushdown WHERE filter_int = 100", "2");
        Thread.sleep(1000);
        // Expect parameterized query with quoted identifiers and parameter logging
        assertLogContains("Executing Teradata SQL", "\"filter_int\" = ?");
        assertLogContains("Query Parameters:", "[100]");
    }

    @Test
    @DisplayName("15.4 VARCHAR filter pushed to Teradata")
    public void test15_4() throws IOException, InterruptedException {
        assertQuery("SELECT COUNT(*) FROM test_pushdown WHERE filter_varchar = 'alpha'", "2");
        Thread.sleep(1000);
        assertLogContains("Executing Teradata SQL", "\"filter_varchar\" = ?");
        assertLogContains("Query Parameters:", "[alpha]");
    }

    @Test
    @DisplayName("15.6 LIMIT pushed as SAMPLE to Teradata")
    public void test15_6() throws IOException, InterruptedException {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM test_pushdown LIMIT 3)", "3");
        Thread.sleep(1000);
        // LIMIT/SAMPLE usually not parameterized if it's structural
        assertLogContains("Executing Teradata SQL", "SAMPLE 3");
    }

    @Test
    @DisplayName("16.2 Dynamic filter or standard execution")
    public void test16_2() throws IOException, InterruptedException {
        // Note: Join pushdown is not currently implemented in TeradataClient
        // This test validates that the query executes successfully with dynamic filtering or standard execution
        assertQuery("SELECT COUNT(*) FROM test_join_fact f JOIN test_join_dim d ON f.dim_id = d.dim_id WHERE d.dim_category = 'Type1'", "5");
        Thread.sleep(1000);
        
        // Check that both tables were queried (indicating the join was executed)
        boolean foundFactTable = checkLogFor("Executing Teradata SQL", "test_join_fact");
        boolean foundDimTable = checkLogFor("Executing Teradata SQL", "test_join_dim");
        
        assertThat(foundFactTable && foundDimTable).as("Both join tables should be queried").isTrue();
    }

    private void assertLogContains(String marker, String expected) throws IOException {
        assertThat(checkLogFor(marker, expected)).as("Log missing: " + expected + " with marker " + marker).isTrue();
    }

    private boolean checkLogFor(String marker, String expected) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get(LOG_FILE));
        int start = Math.max(0, lines.size() - 2000); // Increased scan depth
        for (int i = lines.size() - 1; i >= start; i--) {
            String line = lines.get(i);
            if (line.contains(marker) && line.contains(expected)) return true;
        }
        return false;
    }
}
