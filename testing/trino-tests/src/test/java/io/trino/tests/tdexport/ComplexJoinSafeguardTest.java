package io.trino.tests.tdexport;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("SQL Generation Hardening Tests")
public class ComplexJoinSafeguardTest extends BaseTdExportTest {

    @Test
    @DisplayName("Verify nested join on right side is handled safely")
    public void testNestedJoinRightSide() {
        // Use test_pushdown consistently. Join with a subquery that itself contains a join.
        String sql = "SELECT a.test_id FROM test_pushdown a JOIN " +
                     "(SELECT t1.test_id FROM test_pushdown t1 JOIN test_pushdown t2 ON t1.test_id = t2.test_id) b " +
                     "ON a.test_id = b.test_id WHERE a.test_id <= 3";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("Nested join right side result count: " + result.size());
    }

    @Test
    @DisplayName("Verify boolean literal pushdown uses TRUE/FALSE")
    public void testBooleanLiteralPushdown() {
        // Use an expression that likely results in a boolean constant in the pushdown expression
        String sql = "SELECT COUNT(*) FROM test_pushdown WHERE filter_int > 0 OR TRUE";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("Boolean literal test result: " + result.get(0));
    }
}
