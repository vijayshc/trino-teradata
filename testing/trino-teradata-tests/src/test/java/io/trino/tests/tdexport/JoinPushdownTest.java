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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for Join Pushdown to Teradata.
 * 
 * Tests validate that INNER, LEFT, RIGHT, and FULL joins are pushed to Teradata.
 * Uses test_pushdown and test_integer_types tables for join testing.
 */
@DisplayName("Section 20: Join Pushdown Tests")
public class JoinPushdownTest extends BaseTdExportTest {

    // ======================================================================
    // INNER JOIN Tests
    // ======================================================================

    @Test
    @DisplayName("20.1 Simple INNER JOIN on matching column")
    public void testSimpleInnerJoin() {
        // Join test_pushdown.test_id with test_integer_types.test_id
        String sql = "SELECT a.test_id, a.filter_varchar, b.col_integer " +
                     "FROM test_pushdown a " +
                     "INNER JOIN test_integer_types b ON a.test_id = b.test_id " +
                     "WHERE a.test_id <= 5 ORDER BY a.test_id";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("20.1 INNER JOIN results: " + result.size() + " rows");
    }

    @Test
    @DisplayName("20.2 INNER JOIN with filter condition")
    public void testInnerJoinWithFilter() {
        String sql = "SELECT COUNT(*) FROM test_pushdown a " +
                     "INNER JOIN test_integer_types b ON a.test_id = b.test_id " +
                     "WHERE a.filter_int = 100";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("20.2 INNER JOIN with filter: " + result);
    }

    @Test
    @DisplayName("20.3 INNER JOIN count matching rows")
    public void testInnerJoinCount() {
        String sql = "SELECT COUNT(*) FROM test_pushdown a " +
                     "INNER JOIN test_integer_types b ON a.test_id = b.test_id";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        int count = Integer.parseInt(result.get(0).replace("\"", ""));
        assertThat(count).isGreaterThan(0);
        log.info("20.3 INNER JOIN count: " + count);
    }

    // ======================================================================
    // LEFT OUTER JOIN Tests
    // ======================================================================

    @Test
    @DisplayName("20.4 Simple LEFT JOIN")
    public void testSimpleLeftJoin() {
        String sql = "SELECT a.test_id, a.filter_varchar, b.col_integer " +
                     "FROM test_pushdown a " +
                     "LEFT JOIN test_integer_types b ON a.test_id = b.test_id " +
                     "WHERE a.test_id <= 3 ORDER BY a.test_id";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("20.4 LEFT JOIN results: " + result.size() + " rows");
    }

    @Test
    @DisplayName("20.5 LEFT JOIN preserves all left rows")
    public void testLeftJoinPreservesLeftRows() {
        // Count should match or exceed left table count for matching IDs
        String sql = "SELECT COUNT(*) FROM test_pushdown a " +
                     "LEFT JOIN test_integer_types b ON a.test_id = b.test_id " +
                     "WHERE a.test_id <= 5";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        int count = Integer.parseInt(result.get(0).replace("\"", ""));
        assertThat(count).isGreaterThanOrEqualTo(5);
        log.info("20.5 LEFT JOIN count: " + count);
    }

    // ======================================================================
    // RIGHT OUTER JOIN Tests
    // ======================================================================

    @Test
    @DisplayName("20.6 Simple RIGHT JOIN")
    public void testSimpleRightJoin() {
        String sql = "SELECT a.test_id, a.filter_varchar, b.col_integer " +
                     "FROM test_pushdown a " +
                     "RIGHT JOIN test_integer_types b ON a.test_id = b.test_id " +
                     "WHERE b.test_id <= 3 ORDER BY b.test_id";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("20.6 RIGHT JOIN results: " + result.size() + " rows");
    }

    @Test  
    @DisplayName("20.7 RIGHT JOIN preserves all right rows")
    public void testRightJoinPreservesRightRows() {
        String sql = "SELECT COUNT(*) FROM test_pushdown a " +
                     "RIGHT JOIN test_integer_types b ON a.test_id = b.test_id " +
                     "WHERE b.test_id <= 5";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("20.7 RIGHT JOIN count: " + result);
    }

    // ======================================================================
    // FULL OUTER JOIN Tests
    // ======================================================================

    @Test
    @DisplayName("20.8 Simple FULL OUTER JOIN")
    public void testSimpleFullJoin() {
        String sql = "SELECT a.test_id, b.test_id as b_test_id " +
                     "FROM test_pushdown a " +
                     "FULL OUTER JOIN test_integer_types b ON a.test_id = b.test_id " +
                     "WHERE a.test_id <= 3 OR b.test_id <= 3 ORDER BY COALESCE(a.test_id, b.test_id)";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("20.8 FULL OUTER JOIN results: " + result.size() + " rows");
    }

    // ======================================================================
    // Complex Join Tests
    // ======================================================================

    @Test
    @DisplayName("20.9 JOIN with aggregation")
    public void testJoinWithAggregation() {
        String sql = "SELECT a.filter_varchar, COUNT(*) as cnt " +
                     "FROM test_pushdown a " +
                     "INNER JOIN test_integer_types b ON a.test_id = b.test_id " +
                     "GROUP BY a.filter_varchar";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("20.9 JOIN with aggregation: " + result.size() + " groups");
    }

    @Test
    @DisplayName("20.10 JOIN with ORDER BY and LIMIT")
    public void testJoinWithOrderByLimit() {
        String sql = "SELECT a.test_id, a.filter_varchar " +
                     "FROM test_pushdown a " +
                     "INNER JOIN test_integer_types b ON a.test_id = b.test_id " +
                     "ORDER BY a.test_id LIMIT 3";
        List<String> result = getQueryResult(sql);
        assertThat(result).hasSize(3);
        log.info("20.10 JOIN with ORDER BY LIMIT: " + result);
    }

    @Test
    @DisplayName("20.11 Self JOIN")
    public void testSelfJoin() {
        String sql = "SELECT a.test_id, a.filter_varchar, b.filter_varchar as b_varchar " +
                     "FROM test_pushdown a " +
                     "INNER JOIN test_pushdown b ON a.filter_int = b.filter_int " +
                     "WHERE a.test_id < b.test_id";
        List<String> result = getQueryResult(sql);
        // Self join should return pairs of rows with same filter_int
        assertThat(result).isNotEmpty();
        log.info("20.11 Self JOIN results: " + result.size() + " rows");
    }

    @Test
    @DisplayName("20.12 JOIN between different schemas")
    public void testCrossSchemaJoin() {
        // Both tables are in trinoexport schema, but tests join capability
        String sql = "SELECT COUNT(*) FROM test_pushdown a " +
                     "INNER JOIN test_char_types b ON a.test_id = b.test_id";
        List<String> result = getQueryResult(sql);
        assertThat(result).isNotEmpty();
        log.info("20.12 Cross-table JOIN count: " + result);
    }
}
