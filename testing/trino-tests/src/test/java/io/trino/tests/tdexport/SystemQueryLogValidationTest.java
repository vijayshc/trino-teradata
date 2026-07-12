package io.trino.tests.tdexport;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Section 23: system.query Log Validation")
public class SystemQueryLogValidationTest extends BaseTdExportTest {

    @Test
    @DisplayName("23.1 system.query execution is wrapped with ExportToTrino")
    public void testSystemQueryWrappedByExportUdf() {
        assertQuery(
                "SELECT COUNT(*) FROM TABLE(tdexport.system.query(query => 'SELECT test_id FROM test_pushdown WHERE filter_int = 100'))",
                "2");

        assertLogContains("system.query analyzed successfully");
        assertLogContains("Executing Teradata SQL", "ExportToTrino", "test_pushdown");
    }
}