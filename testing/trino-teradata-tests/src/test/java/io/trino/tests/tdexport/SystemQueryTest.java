/*
 * Copyright 2025-2026 The trino-teradata-direct contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.tests.tdexport;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Section 22: system.query Integration Tests")
public class SystemQueryTest extends BaseTdExportTest {

    @AfterEach
    public void restoreSystemQuerySessionProperty() {
        executeStatement("SET SESSION tdexport.system_query_enabled = true");
    }

    @Test
    @DisplayName("22.1 Basic literal query")
    public void testBasicLiteralQuery() {
        assertQuery(
                "SELECT answer FROM TABLE(tdexport.system.query(query => 'SELECT 1 AS answer'))",
                "1");
    }

    @Test
    @DisplayName("22.2 Push down simple table select")
    public void testTableSelect() {
        assertQuery(
                "SELECT COUNT(*) FROM TABLE(tdexport.system.query(query => 'SELECT test_id FROM test_pushdown WHERE filter_int = 100'))",
                "2");
    }

    @Test
    @DisplayName("22.2a Explicit schema in remote SQL")
    public void testExplicitSchemaQuery() {
        assertQuery(
                "SELECT COUNT(*) FROM TABLE(tdexport.system.query(query => 'SELECT test_id FROM trinoexport.test_pushdown WHERE filter_int = 100'))",
                "2");
    }

    @Test
    @DisplayName("22.3 Pass through CTE")
    public void testCtePassThrough() {
        assertQuery(
                "SELECT COUNT(*) FROM TABLE(tdexport.system.query(query => 'WITH filtered AS (SELECT test_id FROM test_pushdown WHERE filter_int = 200) SELECT * FROM filtered'))",
                "2");
    }

    @Test
    @DisplayName("22.4 Pass through aggregation")
    public void testAggregationPassThrough() {
        assertQuery(
                "SELECT total_rows FROM TABLE(tdexport.system.query(query => 'SELECT COUNT(*) AS total_rows FROM test_join_fact'))",
                "8");
    }

    @Test
    @DisplayName("22.5 Pass through join")
    public void testJoinPassThrough() {
        assertQuery(
                "SELECT COUNT(*) FROM TABLE(tdexport.system.query(query => 'SELECT f.fact_id, d.dim_name FROM test_join_fact f JOIN test_join_dim d ON f.dim_id = d.dim_id WHERE d.dim_category = ''Type1'''))",
                "5");
    }

    @Test
    @DisplayName("22.6 Column aliases are preserved")
    public void testAliasPreservation() {
        List<String> columnNames = getColumnNames(
                "SELECT * FROM TABLE(tdexport.system.query(query => 'SELECT 1 AS custom_id, ''hello'' AS custom_text'))");

        assertThat(columnNames).containsExactly("custom_id", "custom_text");
    }

    @Test
    @DisplayName("22.7 Unicode result set")
    public void testUnicodePassThrough() {
        assertQuery(
                "SELECT col_unicode FROM TABLE(tdexport.system.query(query => 'SELECT col_unicode FROM test_unicode WHERE test_id = 1'))",
                "中文测试");
    }

    @Test
    @DisplayName("22.8 Zero row result")
    public void testZeroRowResult() {
        assertQuery(
                "SELECT COUNT(*) FROM TABLE(tdexport.system.query(query => 'SELECT test_id FROM test_pushdown WHERE 1 = 0'))",
                "0");
    }

    @Test
    @DisplayName("22.9 Decimal, date, time, and timestamp types")
    public void testDecimalAndTemporalPassThrough() {
        assertQuery(
                "SELECT CAST(col_dec_5_2 AS VARCHAR) FROM TABLE(tdexport.system.query(query => 'SELECT col_dec_5_2 FROM test_decimal_types WHERE test_id = 5'))",
                "123.45");

        String expectedDate = getQueryResult(
                "SELECT CAST(col_date AS VARCHAR) FROM test_datetime_edge WHERE test_id = 5").get(0);

        assertQuery(
                "SELECT CAST(col_date AS VARCHAR) FROM TABLE(tdexport.system.query(query => 'SELECT col_date FROM test_datetime_edge WHERE test_id = 5'))",
                expectedDate);

        String expectedTime = getQueryResult(
                "SELECT CAST(col_time AS VARCHAR) FROM test_datetime_edge WHERE test_id = 5").get(0);

        assertQuery(
                "SELECT CAST(col_time AS VARCHAR) FROM TABLE(tdexport.system.query(query => 'SELECT col_time FROM test_datetime_edge WHERE test_id = 5'))",
                expectedTime);

        String expectedTimestamp = getQueryResult(
                "SELECT CAST(col_timestamp AS VARCHAR) FROM test_datetime_types WHERE test_id = 5").get(0);

        assertQuery(
                "SELECT CAST(col_timestamp AS VARCHAR) FROM TABLE(tdexport.system.query(query => 'SELECT col_timestamp FROM test_datetime_types WHERE test_id = 5'))",
                expectedTimestamp);
    }

    @Test
    @DisplayName("22.10 SMALLINT and VARCHAR types")
    public void testSmallintAndVarcharPassThrough() {
        String expectedSmallint = getQueryResult(
                "SELECT CAST(col_smallint AS VARCHAR) FROM test_integer_types WHERE test_id = 6").get(0);

        assertQuery(
                "SELECT CAST(col_smallint AS VARCHAR) FROM TABLE(tdexport.system.query(query => 'SELECT col_smallint FROM test_integer_types WHERE test_id = 6'))",
                expectedSmallint);

        String expectedVarchar = getQueryResult(
                "SELECT col_varchar_255 FROM test_char_types WHERE test_id = 3").get(0);

        assertQuery(
                "SELECT col_varchar_255 FROM TABLE(tdexport.system.query(query => 'SELECT col_varchar_255 FROM test_char_types WHERE test_id = 3'))",
                expectedVarchar);
    }

    @Test
    @DisplayName("22.11 System views with unsupported Teradata types")
    public void testSystemViewWithUnsupportedTypes() {
        assertQuery(
                "SELECT COUNT(*) FROM TABLE(tdexport.system.query(query => 'SELECT * FROM dbc.tablesv WHERE databasename = ''TrinoExport'' AND tablename = ''test_pushdown'''))",
                "1");
    }

    @Test
    @DisplayName("22.12 Disabled via session property")
    public void testSessionDisable() {
        executeStatement("SET SESSION tdexport.system_query_enabled = false");
        assertQueryFails(
                "SELECT * FROM TABLE(tdexport.system.query(query => 'SELECT 1 AS answer'))",
                "system.query is disabled");
    }

    @Test
    @DisplayName("22.13 Empty SQL rejected")
    public void testEmptySqlRejected() {
        assertQueryFails(
                "SELECT * FROM TABLE(tdexport.system.query(query => '   '))",
                "non-empty SQL string");
    }
}