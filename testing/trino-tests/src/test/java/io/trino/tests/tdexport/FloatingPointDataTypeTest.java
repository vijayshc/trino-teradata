package io.trino.tests.tdexport;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Section 4: Floating Point Data Type Tests")
public class FloatingPointDataTypeTest extends BaseTdExportTest {

    @Test
    @DisplayName("4.1 FLOAT zero")
    public void test4_1() {
        assertQuery("SELECT col_float FROM test_float_types WHERE test_id = 1", "0.0");
    }

    @Test
    @DisplayName("4.2 FLOAT negative")
    public void test4_2() {
        assertQuery("SELECT col_float FROM test_float_types WHERE test_id = 3", "-1.0");
    }

    @Test
    @DisplayName("4.3 FLOAT Pi approximation")
    public void test4_3() {
        List<String> results = getQueryResult("SELECT ROUND(col_float, 5) FROM test_float_types WHERE test_id = 4");
        assertThat(results).isNotEmpty();
        assertThat(results.get(0)).contains("3.14159");
    }
}
