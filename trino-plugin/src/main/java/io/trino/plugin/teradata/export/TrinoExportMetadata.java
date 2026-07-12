package io.trino.plugin.teradata.export;

import io.airlift.log.Logger;
import io.trino.plugin.jdbc.*;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.SchemaTableName;

import com.google.inject.Inject;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Teradata Export Connector Metadata with fast hybrid metadata retrieval.
 * Extends DefaultJdbcMetadata to leverage Trino's JDBC infrastructure.
 */
public class TrinoExportMetadata extends DefaultJdbcMetadata {
    private static final Logger log = Logger.get(TrinoExportMetadata.class);
    private final TrinoExportConfig config;
    private final TeradataClient teradataClient;
    private final com.google.common.cache.Cache<SchemaTableName, JdbcTableHandle> tableCache;
    private final com.google.common.cache.Cache<SchemaTableName, java.util.Map<String, io.trino.spi.connector.ColumnHandle>> columnCache;
    private final com.google.common.cache.Cache<String, List<String>> schemaCache;

    @Inject
    public TrinoExportMetadata(
            TeradataClient teradataClient,
            TrinoExportConfig config,
            JdbcMetadataConfig metadataConfig,
            TimestampTimeZoneDomain timestampTimeZoneDomain,
            Set<JdbcQueryEventListener> jdbcQueryEventListeners) {
        super(teradataClient, timestampTimeZoneDomain, true, jdbcQueryEventListeners);
        this.config = config;
        this.teradataClient = teradataClient;
        
        this.tableCache = com.google.common.cache.CacheBuilder.newBuilder()
                .maximumSize(config.getMetadataCacheSize())
                .expireAfterWrite(java.time.Duration.ofHours(1))
                .build();

        this.columnCache = com.google.common.cache.CacheBuilder.newBuilder()
                .maximumSize(config.getMetadataCacheSize())
                .expireAfterWrite(java.time.Duration.ofHours(1))
                .build();

        this.schemaCache = com.google.common.cache.CacheBuilder.newBuilder()
                .maximumSize(config.getMetadataCacheSize() / 10)  // Schema cache is 10% of table cache
                .expireAfterWrite(java.time.Duration.ofHours(1))
                .build();
                
        log.info("TrinoExportMetadata initialized with cache size %d and join pushdown support", config.getMetadataCacheSize());
    }

    @Override
    public JdbcTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion) {
        JdbcTableHandle handle = tableCache.getIfPresent(tableName);
        if (handle != null) {
            log.info("Cache HIT for table: %s", tableName);
            return handle;
        }
        
        log.info("Cache MISS for table: %s - fetching from Teradata", tableName);
        long start = System.currentTimeMillis();
        
        handle = super.getTableHandle(session, tableName, startVersion, endVersion);
        long duration = System.currentTimeMillis() - start;
        log.info("Fetched table %s in %d ms", tableName, duration);
        
        if (handle != null) {
            tableCache.put(tableName, handle);
        }
        return handle;
    }
    
    @Override
    public java.util.Map<String, io.trino.spi.connector.ColumnHandle> getColumnHandles(ConnectorSession session, io.trino.spi.connector.ConnectorTableHandle tableHandle) {
        // Don't cache column handles as it interferes with join pushdown
        // Join pushdown creates dynamic column mappings that shouldn't be cached
        return super.getColumnHandles(session, tableHandle);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        String cacheKey = "SCHEMAS";
        List<String> cachedSchemas = schemaCache.getIfPresent(cacheKey);
        if (cachedSchemas != null) {
            log.info("Cache HIT for schema names");
            return cachedSchemas;
        }

        log.info("Cache MISS for schema names - fetching from Teradata");
        long start = System.currentTimeMillis();
        Set<String> schemas = teradataClient.getSchemaNames(session);
        
        // Add default schemas from config
        Set<String> allSchemas = new LinkedHashSet<>(schemas);
        if (config.getDefaultSchemas() != null) {
            for (String schema : config.getDefaultSchemas().split(",")) {
                String trimmed = schema.trim();
                if (!trimmed.isEmpty()) {
                    allSchemas.add(trimmed);
                }
            }
        }
        long duration = System.currentTimeMillis() - start;
        log.info("Fetched schema names in %d ms: %s", duration, allSchemas);
        
        List<String> result = new ArrayList<>(allSchemas);
        schemaCache.put(cacheKey, result);
        return result;
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
        return teradataClient.getTableNames(session, schemaName);
    }

    @Override
    public Optional<io.trino.spi.connector.ProjectionApplicationResult<io.trino.spi.connector.ConnectorTableHandle>> applyProjection(
            io.trino.spi.connector.ConnectorSession session,
            io.trino.spi.connector.ConnectorTableHandle handle,
            List<io.trino.spi.expression.ConnectorExpression> projections,
            java.util.Map<String, io.trino.spi.connector.ColumnHandle> assignments) {
        // Delegate to parent DefaultJdbcMetadata
        return super.applyProjection(session, handle, projections, assignments);
    }

    @Override
    public Optional<io.trino.spi.connector.AggregationApplicationResult<io.trino.spi.connector.ConnectorTableHandle>> applyAggregation(
            io.trino.spi.connector.ConnectorSession session,
            io.trino.spi.connector.ConnectorTableHandle tableHandle,
            List<io.trino.spi.connector.AggregateFunction> aggregates,
            java.util.Map<String, io.trino.spi.connector.ColumnHandle> assignments,
            List<List<io.trino.spi.connector.ColumnHandle>> groupingSets) {
        // Delegate to parent DefaultJdbcMetadata
        return super.applyAggregation(session, tableHandle, aggregates, assignments, groupingSets);
    }

    public TeradataClient getTeradataClient() {
        return teradataClient;
    }

    /**
     * Update the table cache with a handle.
     * Called by MetadataRefreshService during background refresh.
     */
    public void updateTableCache(SchemaTableName tableName, JdbcTableHandle handle) {
        if (handle != null) {
            tableCache.put(tableName, handle);
            log.debug("Updated table cache for %s", tableName);
        }
    }

    /**
     * Update the schema cache with a list of schemas.
     * Called by MetadataRefreshService during background refresh.
     */
    public void updateSchemaCache(List<String> schemas) {
        if (schemas != null && !schemas.isEmpty()) {
            schemaCache.put("SCHEMAS", schemas);
            log.debug("Updated schema cache with %d schemas", schemas.size());
        }
    }
}
