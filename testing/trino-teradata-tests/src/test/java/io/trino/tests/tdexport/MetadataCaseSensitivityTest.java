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

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

public class MetadataCaseSensitivityTest extends BaseTdExportTest {

    @Test
    public void testLowerCaseTableExistence() {
        // This query should succeed if the connector correctly handles
        // 'dbc.tables' (lowercase) by finding 'DBC.TABLES' (uppercase)
        
        // We use a simple select with limit to just check existence/metadata
        String sql = "SELECT * FROM dbc.tables LIMIT 1";
        
        try (java.sql.Statement stmt = connection.createStatement()) {
            // executeQuery throws SQLException if table doesn't exist
            stmt.executeQuery(sql);
        } catch (java.sql.SQLException e) {
             // Fail if table not found
             assertThat(e.getMessage()).doesNotContain("Table 'tdexport.dbc.tables' does not exist");
             // If other error, rethrow or fail
             throw new RuntimeException(e);
        }
    }

    @Test
    public void testExplainLowerCaseTable() {
        // The specific case user reported
        String sql = "EXPLAIN SELECT * FROM dbc.tables";
        
        try (java.sql.Statement stmt = connection.createStatement()) {
            stmt.executeQuery(sql);
        } catch (java.sql.SQLException e) {
             assertThat(e.getMessage()).doesNotContain("Table 'tdexport.dbc.tables' does not exist");
             throw new RuntimeException(e);
        }
    }

    @Test
    public void testMixedCaseTableExistence() {
        // dbc.TABLES
        String sql = "SELECT * FROM dbc.TABLES LIMIT 1";
        
        try (java.sql.Statement stmt = connection.createStatement()) {
            stmt.executeQuery(sql);
        } catch (java.sql.SQLException e) {
             assertThat(e.getMessage()).doesNotContain("does not exist");
             throw new RuntimeException(e);
        }
    }
}
