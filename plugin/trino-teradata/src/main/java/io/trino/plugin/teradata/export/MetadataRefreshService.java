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
package io.trino.plugin.teradata.export;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.spi.connector.SchemaTableName;

import javax.annotation.PreDestroy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Background service that periodically refreshes metadata caches for configured schemas.
 * This ensures that interactive queries hit warm caches, avoiding expensive Teradata metadata lookups.
 */
public class MetadataRefreshService {
    private static final Logger log = Logger.get(MetadataRefreshService.class);

    private final TrinoExportConfig config;
    private final TeradataClient teradataClient;
    private final TrinoExportMetadata metadata;
    private final ScheduledExecutorService scheduler;
    private volatile boolean running = false;
    
    // Track the last successful refresh timestamp for incremental refresh
    // Uses LastAlterTimestamp from DBC.TablesV to detect modified tables
    private final AtomicReference<Timestamp> lastRefreshTimestamp = new AtomicReference<>(null);


    @Inject
    public MetadataRefreshService(
            TrinoExportConfig config,
            TeradataClient teradataClient,
            TrinoExportMetadata metadata) {
        this.config = config;
        this.teradataClient = teradataClient;
        this.metadata = metadata;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "metadata-refresh-service");
            t.setDaemon(true);
            return t;
        });

        // Start the service immediately (Guice doesn't auto-invoke @PostConstruct)
        start();
    }

    private void start() {
        Duration interval = config.getMetadataRefreshInterval();
        String[] schemas = config.getMetadataRefreshSchemasArray();

        if (interval.toMillis() <= 0 || schemas.length == 0) {
            log.info("MetadataRefreshService is DISABLED (interval=%s, schemas=%d)", 
                    interval, schemas.length);
            return;
        }

        running = true;
        long intervalMs = interval.toMillis();

        log.info("MetadataRefreshService starting: interval=%s, schemas=%s, refreshColumns=%s",
                interval, config.getMetadataRefreshSchemas(), config.isMetadataRefreshColumns());

        // Schedule initial refresh immediately (with 5 second delay for connection init)
        scheduler.schedule(this::refreshMetadata, 5, TimeUnit.SECONDS);

        // Schedule periodic refresh
        scheduler.scheduleAtFixedRate(this::refreshMetadata, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    public void stop() {
        running = false;
        scheduler.shutdownNow();
        log.info("MetadataRefreshService stopped");
    }

    private void refreshMetadata() {
        if (!running) return;

        String[] schemas = config.getMetadataRefreshSchemasArray();
        Timestamp previousRefreshTime = lastRefreshTimestamp.get();
        boolean isIncrementalRefresh = (previousRefreshTime != null);
        
        log.info("Starting %s metadata refresh for %d schemas%s", 
                isIncrementalRefresh ? "INCREMENTAL" : "FULL",
                schemas.length,
                isIncrementalRefresh ? " (since " + previousRefreshTime + ")" : "");
        
        long startTime = System.currentTimeMillis();
        int totalTables = 0;
        int totalColumns = 0;
        Timestamp currentRefreshTime = null;

        Connection conn = null;
        try {
            log.info("Opening connection for metadata refresh...");
            conn = java.sql.DriverManager.getConnection(
                    config.getTeradataUrl(),
                    config.getTeradataUser(),
                    config.getTeradataPassword());
            log.info("Connection opened successfully");
            
            // Get current timestamp from Teradata before starting refresh
            // This will be used as the baseline for the next incremental refresh
            currentRefreshTime = getCurrentTeradataTimestamp(conn);
            log.info("Current Teradata timestamp: %s", currentRefreshTime);
            
            // Refresh schema list (fast single query) - always do this
            long schemaStart = System.currentTimeMillis();
            List<String> schemaList = fetchSchemas(conn);
            log.info("Fetched %d schemas in %d ms", schemaList.size(), System.currentTimeMillis() - schemaStart);
            if (!schemaList.isEmpty()) {
                metadata.updateSchemaCache(schemaList);
            }

            // Build IN clause for configured schemas
            StringBuilder schemaFilter = new StringBuilder();
            for (int i = 0; i < schemas.length; i++) {
                if (i > 0) schemaFilter.append(",");
                schemaFilter.append("'").append(schemas[i].trim().toUpperCase().replace("'", "''")).append("'");
            }

            // Fetch table handles - either full or incremental based on last refresh time
            long tableStart = System.currentTimeMillis();
            Map<SchemaTableName, String> tableKinds;
            Set<SchemaTableName> modifiedTables;
            
            if (isIncrementalRefresh) {
                // INCREMENTAL: Only fetch tables modified since last refresh
                IncrementalRefreshResult result = refreshTableHandlesIncremental(conn, schemaFilter.toString(), previousRefreshTime);
                tableKinds = result.tableKinds;
                modifiedTables = result.modifiedTables;
                totalTables = modifiedTables.size();
                log.info("Incremental refresh: found %d modified tables (out of %d total) in %d ms", 
                        modifiedTables.size(), tableKinds.size(), System.currentTimeMillis() - tableStart);
            } else {
                // FULL: Fetch all tables
                tableKinds = refreshTableHandles(conn, schemaFilter.toString());
                modifiedTables = tableKinds.keySet();
                totalTables = tableKinds.size();
                log.info("Full refresh: cached %d table handles in %d ms", totalTables, System.currentTimeMillis() - tableStart);
            }

            // Update column metadata cache - only for modified tables
            if (config.isMetadataRefreshColumns() && !modifiedTables.isEmpty()) {
                long columnStart = System.currentTimeMillis();
                totalColumns = refreshColumnMetadataForTables(conn, schemaFilter.toString(), tableKinds, modifiedTables);
                log.info("Refreshed %d columns for %d modified tables in %d ms", 
                        totalColumns, modifiedTables.size(), System.currentTimeMillis() - columnStart);
            }
            
            // Update the last refresh timestamp on successful completion
            if (currentRefreshTime != null) {
                lastRefreshTimestamp.set(currentRefreshTime);
                log.debug("Updated last refresh timestamp to %s", currentRefreshTime);
            }

        } catch (Exception e) {
            log.error(e, "Failed during metadata refresh: %s", e.getMessage());
            return;
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    log.warn("Failed to close connection: %s", e.getMessage());
                }
            }
        }

        long duration = System.currentTimeMillis() - startTime;
        log.info("Metadata refresh completed (%s): %d schemas, %d tables, %d columns in %d ms",
                isIncrementalRefresh ? "INCREMENTAL" : "FULL",
                schemas.length, totalTables, totalColumns, duration);
    }
    
    /**
     * Get the current timestamp from Teradata server.
     * This ensures we use Teradata's clock, not the local system clock.
     */
    private Timestamp getCurrentTeradataTimestamp(Connection conn) {
        String sql = "SELECT CURRENT_TIMESTAMP";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getTimestamp(1);
            }
        } catch (SQLException e) {
            log.warn("Failed to get current Teradata timestamp: %s", e.getMessage());
        }
        // Fallback to current system time if Teradata query fails
        return new Timestamp(System.currentTimeMillis());
    }
    
    /**
     * Result container for incremental refresh operation.
     */
    private static class IncrementalRefreshResult {
        final Map<SchemaTableName, String> tableKinds;
        final Set<SchemaTableName> modifiedTables;
        
        IncrementalRefreshResult(Map<SchemaTableName, String> tableKinds, Set<SchemaTableName> modifiedTables) {
            this.tableKinds = tableKinds;
            this.modifiedTables = modifiedTables;
        }
    }
    
    /**
     * Fetch table handles with incremental refresh support.
     * Returns all tables for cache consistency, but identifies which ones were modified.
     */
    private IncrementalRefreshResult refreshTableHandlesIncremental(Connection conn, String schemaFilter, Timestamp lastRefreshTime) {
        Map<SchemaTableName, String> tableKinds = new HashMap<>();
        Set<SchemaTableName> modifiedTables = new HashSet<>();
        
        // Query includes LastAlterTimestamp to detect modified tables
        String sql = String.format(
                "SELECT DatabaseName, TableName, TableKind, LastAlterTimestamp " +
                "FROM DBC.TablesV " +
                "WHERE DatabaseName IN (%s) AND TableKind IN ('T', 'V', 'O') " +
                "ORDER BY DatabaseName, TableName",
                schemaFilter);
        
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                String schemaName = rs.getString(1).trim();
                String tableName = rs.getString(2).trim();
                String tableKind = rs.getString(3).trim();
                Timestamp alterTimestamp = rs.getTimestamp(4);
                
                SchemaTableName schemaTableName = new SchemaTableName(schemaName.toLowerCase(), tableName.toLowerCase());
                tableKinds.put(schemaTableName, tableKind);
                
                // Check if this table was modified since last refresh
                boolean isModified = (alterTimestamp != null && alterTimestamp.after(lastRefreshTime));
                
                if (isModified) {
                    modifiedTables.add(schemaTableName);
                    log.debug("Table %s was modified at %s (after %s)", schemaTableName, alterTimestamp, lastRefreshTime);
                    
                    // Update cache for modified tables
                    RemoteTableName remoteTableName = new RemoteTableName(
                            Optional.empty(),
                            Optional.of(schemaName),
                            tableName);
                    JdbcTableHandle tableHandle = new JdbcTableHandle(
                            schemaTableName,
                            remoteTableName,
                            Optional.empty());
                    metadata.updateTableCache(schemaTableName, tableHandle);
                }
            }
        } catch (SQLException e) {
            log.warn("Failed to fetch tables for incremental refresh: %s", e.getMessage());
        }
        
        return new IncrementalRefreshResult(tableKinds, modifiedTables);
    }

    private List<String> fetchSchemas(Connection conn) {
        List<String> schemas = new ArrayList<>();
        String sql = "SELECT DISTINCT DatabaseName FROM DBC.Databases WHERE DBKind IN ('D', 'U') ORDER BY DatabaseName";
        
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                String schemaName = rs.getString(1);
                if (schemaName != null) {
                    schemas.add(schemaName.trim().toLowerCase());
                }
            }
        } catch (SQLException e) {
            log.warn("Failed to fetch schemas: %s", e.getMessage());
        }
        return schemas;
    }

    private Map<SchemaTableName, String> refreshTableHandles(Connection conn, String schemaFilter) {
        Map<SchemaTableName, String> tableKinds = new HashMap<>();
        String sql = String.format(
                "SELECT DatabaseName, TableName, TableKind FROM DBC.TablesV WHERE DatabaseName IN (%s) AND TableKind IN ('T', 'V', 'O') ORDER BY DatabaseName, TableName",
                schemaFilter);
        
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                String schemaName = rs.getString(1).trim();
                String tableName = rs.getString(2).trim();
                String tableKind = rs.getString(3).trim();
                
                SchemaTableName schemaTableName = new SchemaTableName(schemaName.toLowerCase(), tableName.toLowerCase());
                RemoteTableName remoteTableName = new RemoteTableName(
                        Optional.empty(),
                        Optional.of(schemaName),
                        tableName);
                JdbcTableHandle tableHandle = new JdbcTableHandle(
                        schemaTableName,
                        remoteTableName,
                        Optional.empty());
                metadata.updateTableCache(schemaTableName, tableHandle);
                tableKinds.put(schemaTableName, tableKind);
            }
        } catch (SQLException e) {
            log.warn("Failed to fetch tables: %s", e.getMessage());
        }
        return tableKinds;
    }

    private int refreshColumnMetadata(Connection conn, String schemaFilter, Map<SchemaTableName, String> tableKinds) {
        int count = 0;
        // Two-level fetching:
        
        // Level 1: Bulk fetch columns from DBC.ColumnsV for Tables only (TableKind='T')
        String sql = String.format(
                "SELECT c.DatabaseName, c.TableName, c.ColumnName, c.ColumnType, c.ColumnLength, c.DecimalTotalDigits, c.DecimalFractionalDigits, c.Nullable " +
                "FROM DBC.ColumnsV c " +
                "JOIN DBC.TablesV t ON c.DatabaseName = t.DatabaseName AND c.TableName = t.TableName " +
                "WHERE c.DatabaseName IN (%s) AND t.TableKind = 'T' " +
                "ORDER BY c.DatabaseName, c.TableName, c.ColumnId",
                schemaFilter);
        
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            String currentSchema = null;
            String currentTable = null;
            List<JdbcColumnHandle> columns = new ArrayList<>();
            
            while (rs.next()) {
                String schemaName = safeTrim(rs.getString(1));
                String tableName = safeTrim(rs.getString(2));
                
                // If we've moved to a new table, save the previous table's columns
                if (!schemaName.equalsIgnoreCase(currentSchema) || !tableName.equalsIgnoreCase(currentTable)) {
                    if (currentSchema != null && !columns.isEmpty()) {
                        SchemaTableName prevTable = new SchemaTableName(currentSchema.toLowerCase(), currentTable.toLowerCase());
                        teradataClient.updateMetadataCache(prevTable, new ArrayList<>(columns));
                    }
                    columns.clear();
                    currentSchema = schemaName;
                    currentTable = tableName;
                }
                
                String columnName = safeTrim(rs.getString(3));
                String columnType = safeTrim(rs.getString(4));
                int columnLength = rs.getInt(5);
                int precision = rs.getInt(6);
                int scale = rs.getInt(7);
                String nullable = safeTrim(rs.getString(8));
                
                // Map Teradata type to JDBC type
                int jdbcType = mapTeradataTypeToJdbc(columnType);
                io.trino.plugin.jdbc.JdbcTypeHandle typeHandle = new io.trino.plugin.jdbc.JdbcTypeHandle(
                        jdbcType,
                        Optional.of(columnType),
                        Optional.of(precision > 0 ? precision : columnLength),
                        Optional.of(scale),
                        Optional.empty(),
                        Optional.empty());

                Optional<io.trino.plugin.jdbc.ColumnMapping> columnMapping = 
                        teradataClient.toColumnMapping(null, conn, typeHandle);

                if (columnMapping.isPresent()) {
                    columns.add(JdbcColumnHandle.builder()
                            .setColumnName(columnName.toLowerCase())
                            .setJdbcTypeHandle(typeHandle)
                            .setColumnType(columnMapping.get().getType())
                            .setNullable("Y".equalsIgnoreCase(nullable))
                            .build());
                    count++;
                }
            }
            
            // Save column metadata for the LAST table in the loop
            if (currentSchema != null && !columns.isEmpty()) {
                SchemaTableName lastTable = new SchemaTableName(currentSchema.toLowerCase(), currentTable.toLowerCase());
                teradataClient.updateMetadataCache(lastTable, new ArrayList<>(columns));
            }
            
        } catch (SQLException e) {
            log.warn("Failed to bulk fetch columns: %s", e.getMessage());
        }

        // Level 2: For Views and other types, use direct fetching (SELECT * FROM ... WHERE 1=0)
        for (Map.Entry<SchemaTableName, String> entry : tableKinds.entrySet()) {
            String kind = entry.getValue();
            if ("V".equalsIgnoreCase(kind) || "O".equalsIgnoreCase(kind)) {
                SchemaTableName schemaTableName = entry.getKey();
                try {
                    RemoteTableName remoteTableName = new RemoteTableName(
                            Optional.empty(), 
                            Optional.of(schemaTableName.getSchemaName()), 
                            schemaTableName.getTableName());
                    
                    List<JdbcColumnHandle> viewColumns = teradataClient.getColumns(conn, null, schemaTableName, remoteTableName);
                    if (viewColumns != null && !viewColumns.isEmpty()) {
                        teradataClient.updateMetadataCache(schemaTableName, viewColumns);
                        count += viewColumns.size();
                    }
                } catch (Exception e) {
                    log.warn("Failed direct column refresh for view %s: %s", schemaTableName, e.getMessage());
                }
            }
        }
        
        return count;
    }

    /**
     * Refresh column metadata only for specific tables.
     * This is used during incremental refresh to update columns only for modified tables.
     */
    private int refreshColumnMetadataForTables(Connection conn, String schemaFilter, 
            Map<SchemaTableName, String> tableKinds, Set<SchemaTableName> tablesToRefresh) {
        
        if (tablesToRefresh.isEmpty()) {
            return 0;
        }
        
        int count = 0;
        
        // Separate tables into physical tables (T) and views/other (V, O)
        Set<SchemaTableName> physicalTables = new HashSet<>();
        Set<SchemaTableName> viewsAndOther = new HashSet<>();
        
        for (SchemaTableName table : tablesToRefresh) {
            String kind = tableKinds.get(table);
            if ("T".equalsIgnoreCase(kind)) {
                physicalTables.add(table);
            } else {
                viewsAndOther.add(table);
            }
        }
        
        // For physical tables, build an optimized query with table filter
        if (!physicalTables.isEmpty()) {
            StringBuilder tableFilter = new StringBuilder();
            for (SchemaTableName table : physicalTables) {
                if (tableFilter.length() > 0) {
                    tableFilter.append(" OR ");
                }
                tableFilter.append("(c.DatabaseName = '")
                        .append(table.getSchemaName().toUpperCase().replace("'", "''"))
                        .append("' AND c.TableName = '")
                        .append(table.getTableName().toUpperCase().replace("'", "''"))
                        .append("')");
            }
            
            String sql = String.format(
                    "SELECT c.DatabaseName, c.TableName, c.ColumnName, c.ColumnType, c.ColumnLength, " +
                    "c.DecimalTotalDigits, c.DecimalFractionalDigits, c.Nullable " +
                    "FROM DBC.ColumnsV c " +
                    "WHERE (%s) " +
                    "ORDER BY c.DatabaseName, c.TableName, c.ColumnId",
                    tableFilter.toString());
            
            log.debug("Refreshing columns for %d physical tables", physicalTables.size());
            
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                
                String currentSchema = null;
                String currentTable = null;
                List<JdbcColumnHandle> columns = new ArrayList<>();
                
                while (rs.next()) {
                    String schemaName = safeTrim(rs.getString(1));
                    String tableName = safeTrim(rs.getString(2));
                    
                    if (!schemaName.equalsIgnoreCase(currentSchema) || !tableName.equalsIgnoreCase(currentTable)) {
                        if (currentSchema != null && !columns.isEmpty()) {
                            SchemaTableName prevTable = new SchemaTableName(currentSchema.toLowerCase(), currentTable.toLowerCase());
                            teradataClient.updateMetadataCache(prevTable, new ArrayList<>(columns));
                        }
                        columns.clear();
                        currentSchema = schemaName;
                        currentTable = tableName;
                    }
                    
                    String columnName = safeTrim(rs.getString(3));
                    String columnType = safeTrim(rs.getString(4));
                    int columnLength = rs.getInt(5);
                    int precision = rs.getInt(6);
                    int scale = rs.getInt(7);
                    String nullable = safeTrim(rs.getString(8));
                    
                    int jdbcType = mapTeradataTypeToJdbc(columnType);
                    io.trino.plugin.jdbc.JdbcTypeHandle typeHandle = new io.trino.plugin.jdbc.JdbcTypeHandle(
                            jdbcType,
                            Optional.of(columnType),
                            Optional.of(precision > 0 ? precision : columnLength),
                            Optional.of(scale),
                            Optional.empty(),
                            Optional.empty());

                    Optional<io.trino.plugin.jdbc.ColumnMapping> columnMapping = 
                            teradataClient.toColumnMapping(null, conn, typeHandle);

                    if (columnMapping.isPresent()) {
                        columns.add(JdbcColumnHandle.builder()
                                .setColumnName(columnName.toLowerCase())
                                .setJdbcTypeHandle(typeHandle)
                                .setColumnType(columnMapping.get().getType())
                                .setNullable("Y".equalsIgnoreCase(nullable))
                                .build());
                        count++;
                    }
                }
                
                if (currentSchema != null && !columns.isEmpty()) {
                    SchemaTableName lastTable = new SchemaTableName(currentSchema.toLowerCase(), currentTable.toLowerCase());
                    teradataClient.updateMetadataCache(lastTable, new ArrayList<>(columns));
                }
                
            } catch (SQLException e) {
                log.warn("Failed to fetch columns for modified tables: %s", e.getMessage());
            }
        }
        
        // For views and other types, use direct fetching (SELECT * FROM ... WHERE 1=0)
        for (SchemaTableName schemaTableName : viewsAndOther) {
            try {
                RemoteTableName remoteTableName = new RemoteTableName(
                        Optional.empty(), 
                        Optional.of(schemaTableName.getSchemaName()), 
                        schemaTableName.getTableName());
                
                List<JdbcColumnHandle> viewColumns = teradataClient.getColumns(conn, null, schemaTableName, remoteTableName);
                if (viewColumns != null && !viewColumns.isEmpty()) {
                    teradataClient.updateMetadataCache(schemaTableName, viewColumns);
                    count += viewColumns.size();
                }
            } catch (Exception e) {
                log.warn("Failed direct column refresh for view %s: %s", schemaTableName, e.getMessage());
            }
        }
        
        return count;
    }


    private int mapTeradataTypeToJdbc(String teradataType) {
        if (teradataType == null) return java.sql.Types.VARCHAR;
        String type = teradataType.toUpperCase().trim();
        
        // Use exact equals for Teradata type codes to avoid "startsWith" greedy matches
        if (type.equals("I1")) return java.sql.Types.TINYINT;
        if (type.equals("I2")) return java.sql.Types.SMALLINT;
        if (type.equals("I")) return java.sql.Types.INTEGER;
        if (type.equals("I8")) return java.sql.Types.BIGINT;
        if (type.equals("DA")) return java.sql.Types.DATE;
        if (type.equals("TS")) return java.sql.Types.TIMESTAMP;
        if (type.equals("AT")) return java.sql.Types.TIME;
        if (type.equals("D")) return java.sql.Types.DECIMAL;
        if (type.equals("F")) return java.sql.Types.DOUBLE;
        if (type.equals("CV")) return java.sql.Types.VARCHAR;
        if (type.equals("CF")) return java.sql.Types.CHAR;
        if (type.equals("BF")) return java.sql.Types.BINARY;
        if (type.equals("BV")) return java.sql.Types.VARBINARY;
        if (type.equals("CO")) return java.sql.Types.CLOB;
        if (type.equals("BO")) return java.sql.Types.BLOB;

        // Fallback for full type names
        if (type.equals("BYTEINT")) return java.sql.Types.TINYINT;
        if (type.equals("SMALLINT")) return java.sql.Types.SMALLINT;
        if (type.equals("INTEGER")) return java.sql.Types.INTEGER;
        if (type.equals("BIGINT")) return java.sql.Types.BIGINT;
        if (type.equals("DECIMAL")) return java.sql.Types.DECIMAL;
        if (type.equals("FLOAT") || type.equals("REAL") || type.equals("DOUBLE")) return java.sql.Types.DOUBLE;
        if (type.equals("VARCHAR")) return java.sql.Types.VARCHAR;
        if (type.equals("CHAR")) return java.sql.Types.CHAR;
        if (type.equals("DATE")) return java.sql.Types.DATE;
        if (type.equals("TIMESTAMP")) return java.sql.Types.TIMESTAMP;
        if (type.equals("TIME")) return java.sql.Types.TIME;
        if (type.equals("BYTE")) return java.sql.Types.BINARY;
        if (type.equals("VARBYTE")) return java.sql.Types.VARBINARY;
        if (type.equals("CLOB")) return java.sql.Types.CLOB;
        if (type.equals("BLOB")) return java.sql.Types.BLOB;
        
        return java.sql.Types.VARCHAR; // Default fallback
    }

    private String safeTrim(String value) {
        return value == null ? "" : value.trim();
    }
}

