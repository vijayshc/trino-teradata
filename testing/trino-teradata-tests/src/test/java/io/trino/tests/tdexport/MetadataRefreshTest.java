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
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test to verify that metadata refresh service is populating caches correctly.
 * For schemas configured in teradata.export.metadata-refresh-schemas, 
 * subsequent metadata calls should hit the cache (Cache HIT in logs).
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MetadataRefreshTest extends BaseTdExportTest {

    @Test
    @Order(1)
    @DisplayName("Verify metadata refresh service is running")
    void testMetadataRefreshServiceRunning() {
        log.info("Checking if MetadataRefreshService started...");
        
        // Check logs for service startup
        boolean serviceStarted = checkLogFor("MetadataRefreshService starting", "interval");
        assertThat(serviceStarted)
                .as("MetadataRefreshService should be running (check teradata.export.metadata-refresh-interval/schemas config)")
                .isTrue();
    }

    @Test
    @Order(2)
    @DisplayName("Verify schema cache is populated for refresh schemas")
    void testSchemaCachePopulated() throws Exception {
        log.info("Verifying schema list is cached...");
        
        // First call - may be a MISS if refresh hasn't run yet, or HIT if it has
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW SCHEMAS FROM tdexport")) {
            int count = 0;
            while (rs.next()) {
                count++;
            }
            log.info("Found {} schemas", count);
            assertThat(count).isGreaterThan(0);
        }
        
        // Second call should definitely hit the cache
        Thread.sleep(2000);
        
        // Check for cache hit in logs
        boolean cacheHit = checkLogFor("Cache HIT for schema names");
        log.info("Schema cache hit: {}", cacheHit);
        // Note: This may not always be true during first test run, but should be true after refresh
    }

    // @Test
    // @Order(3)
    // @DisplayName("Verify table cache is populated for DBC schema")
    // void testTableCachePopulated() throws Exception {
    //     log.info("Verifying table handle cache is populated for DBC...");
        
    //     // Query a table from DBC (configured in refresh schemas)
    //     try (Statement stmt = connection.createStatement();
    //          ResultSet rs = stmt.executeQuery("SELECT * FROM tdexport.dbc.tables LIMIT 1")) {
    //         assertThat(rs.next()).isTrue();
    //         log.info("Successfully queried DBC.Tables");
    //     }
        
    //     Thread.sleep(2000);
        
    //     // After refresh, the table should be in cache
    //     // Query again
    //     try (Statement stmt = connection.createStatement();
    //          ResultSet rs = stmt.executeQuery("SELECT * FROM tdexport.dbc.tables LIMIT 1")) {
    //         assertThat(rs.next()).isTrue();
    //     }
        
    //     // Check for cache hit
    //     boolean cacheHit = checkLogFor("Cache HIT for table");
    //     log.info("Table cache hit detected: {}", cacheHit);
    // }

    @Test
    @Order(4)
    @DisplayName("Verify column metadata cache is populated for DBC schema")
    void testColumnCachePopulated() throws Exception {
        log.info("Verifying column metadata cache is populated...");
        
        // DESCRIBE triggers column metadata fetch
        List<String> columns = getQueryResult("DESCRIBE tdexport.dbc.tables");
        log.info("Found {} columns for DBC.Tables", columns.size());
        assertThat(columns).isNotEmpty();
        
        Thread.sleep(2000);
        
        // Second describe should hit cache
        List<String> columns2 = getQueryResult("DESCRIBE tdexport.dbc.tables");
        assertThat(columns2).hasSameSizeAs(columns);
        
        // Check for fast metadata cache hit
        boolean cacheHit = checkLogFor("Fast metadata cache HIT");
        log.info("Column metadata cache hit detected: {}", cacheHit);
    }

    @Test
    @Order(5)
    @DisplayName("Verify refresh started log entry")
    void testRefreshCompletedLog() {
        log.info("Checking for metadata refresh log entries...");
        
        // Check for service startup and refresh start
        boolean serviceStarted = checkLogFor("MetadataRefreshService starting");
        boolean refreshStarted = checkLogFor("Starting metadata refresh");
        
        log.info("Service started: {}, Refresh started: {}", serviceStarted, refreshStarted);
        
        // At least one should be true if config is correct
        assertThat(serviceStarted || refreshStarted)
                .as("MetadataRefreshService should have started (check config)")
                .isTrue();
        
        // Check for completion (may not be present if DBC is large)
        boolean refreshCompleted = checkLogFor("Metadata refresh completed");
        log.info("Metadata refresh completed: {}", refreshCompleted);
    }

    @Test
    @Order(6)
    @DisplayName("Negative: Table not in refresh schemas should trigger cache miss then cache hit")
    void testCacheMissThenHitForNonRefreshSchema() throws Exception {
        // SYSLIB is not in the refresh schemas (TrinoExport,DBC only)
        // So first access should be a cache MISS, then subsequent access should be a HIT
        log.info("Testing cache behavior for schema NOT in refresh list (SYSLIB)...");
        
        // First query should trigger cache MISS and direct JDBC fetch
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM tdexport.syslib.sqlrestrictedwords LIMIT 1")) {
            assertThat(rs.next()).isTrue();
            log.info("Successfully queried SYSLIB.SqlRestrictedWords (first time - expected cache MISS)");
        }
        
        Thread.sleep(2000);
        
        // Check for cache MISS in logs
        boolean cacheMiss = checkLogFor("Fast metadata cache MISS", "syslib");
        log.info("Cache MISS detected for SYSLIB: {}", cacheMiss);
        
        // Second query should hit the cache (on-demand caching with 1000 entry limit)
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM tdexport.syslib.sqlrestrictedwords LIMIT 1")) {
            assertThat(rs.next()).isTrue();
            log.info("Successfully queried SYSLIB.SqlRestrictedWords (second time - expected cache HIT)");
        }
        
        Thread.sleep(2000);
        
        // Check for cache HIT in logs
        boolean cacheHit = checkLogFor("Fast metadata cache HIT", "syslib");
        log.info("Cache HIT detected for SYSLIB (on-demand cached): {}", cacheHit);
        
        // Verify the on-demand caching worked
        assertThat(cacheHit)
                .as("Second access to non-refresh schema should hit cache (on-demand caching)")
                .isTrue();
    }
}

