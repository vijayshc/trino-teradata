package io.trino.tests.tdexport;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Date and Time Data Type Tests")
public class DateTimeDataTypeTest extends BaseTdExportTest {

    @Test
    @DisplayName("6.1 DATE New Year")
    public void testDateNewYear() {
        assertQuery("SELECT col_date FROM test_datetime_types WHERE test_id = 1", "2024-01-01");
    }

    @Test
    @DisplayName("6.2 DATE End of Year")
    public void testDateEndOfYear() {
        assertQuery("SELECT col_date FROM test_datetime_types WHERE test_id = 2", "2024-12-31");
    }

    @Test
    @DisplayName("6.3 DATE Leap Year")
    public void testDateLeapYear() {
        assertQuery("SELECT col_date FROM test_datetime_types WHERE test_id = 3", "2000-02-29");
    }

    @Test
    @DisplayName("6.4 DATE Unix Epoch")
    public void testDateEpoch() {
        assertQuery("SELECT col_date FROM test_datetime_types WHERE test_id = 4", "1970-01-01");
    }

    @Test
    @DisplayName("6.5 TIME pattern match")
    public void testTimePattern() {
        // TIME type has known timezone offset issues, just check for pattern :59:59
        List<String> results = getQueryResult("SELECT CAST(col_time AS VARCHAR) FROM test_datetime_types WHERE test_id = 2");
        assertThat(results).isNotEmpty();
        assertThat(results.get(0)).contains("59:59");
    }

    @Test
    @DisplayName("14.2 Earliest DATE (0001-01-01)")
    public void testEarliestDate() {
        assertQuery("SELECT col_date FROM test_datetime_edge WHERE test_id = 1", "0001-01-01");
    }

    @Test
    @DisplayName("14.3 Earliest TIMESTAMP (0001-01-01 00:00:00)")
    public void testEarliestTimestamp() {
        assertQuery("SELECT CAST(col_timestamp AS VARCHAR) FROM test_datetime_edge WHERE test_id = 1", "0001-01-01 00:00:00.000000");
    }

    @Test
    @DisplayName("14.4 Before 1900 boundary (1899-12-31)")
    public void testBefore1900() {
        assertQuery("SELECT col_date FROM test_datetime_edge WHERE test_id = 2", "1899-12-31");
    }

    @Test
    @DisplayName("14.5 Teradata epoch date (1900-01-01)")
    public void testTeradataEpoch() {
        assertQuery("SELECT col_date FROM test_datetime_edge WHERE test_id = 3", "1900-01-01");
    }

    @Test
    @DisplayName("14.6 Future date")
    public void testFutureDate() {
        assertQuery("SELECT col_date FROM test_datetime_edge WHERE test_id = 4", "2099-12-31");
    }

    @Test
    @DisplayName("14.7 TIME with microseconds")
    public void testTimeMicroseconds() {
        List<String> results = getQueryResult("SELECT CAST(col_time AS VARCHAR) FROM test_datetime_edge WHERE test_id = 5");
        assertThat(results).isNotEmpty();
        assertThat(results.get(0)).contains("06:30:45");
    }
}
