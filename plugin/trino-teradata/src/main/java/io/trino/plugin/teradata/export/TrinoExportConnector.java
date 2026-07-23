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

import io.airlift.log.Logger;
import io.trino.spi.connector.*;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;

import com.google.inject.Inject;
import java.util.List;
import java.util.Set;

public class TrinoExportConnector implements Connector {
    private static final Logger log = Logger.get(TrinoExportConnector.class);
    private final TrinoExportSplitManager splitManager;
    private final TrinoExportFlightServer flightServer;
    private final TeradataBridgeServer bridgeServer;
    private final TrinoExportMetadata metadata;
    private final TrinoExportConfig config;
    private final TrinoExportSessionProperties sessionProperties;
    private final TeradataConnectionPool connectionPool;
    private final Set<ConnectorTableFunction> tableFunctions;

    @Inject
    public TrinoExportConnector(
            TrinoExportSplitManager splitManager,
            TrinoExportFlightServer flightServer,
            TeradataBridgeServer bridgeServer,
            TrinoExportMetadata metadata,
            TrinoExportConfig config,
            TrinoExportSessionProperties sessionProperties,
            TeradataConnectionPool connectionPool,
            Set<ConnectorTableFunction> tableFunctions) {
        this.splitManager = splitManager;
        this.flightServer = flightServer;
        this.bridgeServer = bridgeServer;
        this.metadata = metadata;
        this.config = config;
        this.sessionProperties = sessionProperties;
        this.connectionPool = connectionPool;
        this.tableFunctions = tableFunctions;
        
        // Initialize DataBufferRegistry with configured queue capacity
        DataBufferRegistry.setBufferQueueCapacity(config.getBufferQueueCapacity());
        
        // Initialize timezone offset for DirectTrinoPageParser
        int tzOffsetSeconds = DirectTrinoPageParser.parseTimezoneToSeconds(config.getTeradataTimezone());
        DirectTrinoPageParser.setTeradataTimezoneOffset(tzOffsetSeconds);
        
        log.info("TrinoExportConnector initialized with: bufferQueueCapacity=%d, pagePollTimeout=%dms, debugLogging=%s, timezone=%s (%d seconds)",
                config.getBufferQueueCapacity(), config.getPagePollTimeoutMs(), config.isEnableDebugLogging(),
                config.getTeradataTimezone(), tzOffsetSeconds);
        
        // Start bridge server (hot path). Flight is legacy and optional — bind failure must not
        // take down the catalog (port conflicts are common on multi-node labs).
        bridgeServer.start();
        try {
            flightServer.start();
        } catch (java.io.IOException e) {
            log.warn(e, "Legacy Flight server failed to start (unused on hot path); continuing with bridge only");
        }
    }



    @Override
    public ConnectorSplitManager getSplitManager() {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider() {
        return (transaction, session, split, table, columns, dynamicFilter) -> {
            TrinoExportSplit exportSplit = (TrinoExportSplit) split;
            
            // Get columns from JdbcTableHandle (SQL order - matches binary data from Teradata)
            io.trino.plugin.jdbc.JdbcTableHandle tableHandle = (io.trino.plugin.jdbc.JdbcTableHandle) table;
            List<ColumnHandle> sqlOrderedColumns = tableHandle.getColumns()
                    .filter(list -> !list.isEmpty())
                    .<List<ColumnHandle>>map(list -> list.stream()
                            .map(col -> (ColumnHandle) col)
                            .collect(java.util.stream.Collectors.toList()))
                    .orElse(columns);
            
            // 'columns' parameter is what Trino expects in output order
            // 'sqlOrderedColumns' is how binary data arrives from Teradata
            // PageSource needs both to: 1) parse correctly, 2) reorder output
            return new TrinoExportPageSource(
                    exportSplit.getQueryId(), 
                    sqlOrderedColumns,  // For schema registration (parsing)
                    columns,            // For output reordering
                    config.getTeradataTimezone(),
                    config.getPagePollTimeoutMs(),
                    config.isEnableDebugLogging(),
                    config.getSplitsPerWorker(),
                    exportSplit.getToken());
        };
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties() {
        return sessionProperties.getSessionProperties();
    }

    @Override
    public Set<ConnectorTableFunction> getTableFunctions() {
        if (!config.isSystemQueryEnabled()) {
            return Set.of();
        }
        return tableFunctions;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
        return TrinoExportTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle) {
        log.debug("getMetadata called for query %s", session.getQueryId());
        return metadata;
    }

    @Override
    public void shutdown() {
        log.info("Shutting down TrinoExportConnector...");
        
        // Shutdown the DataBufferRegistry reaper thread and clean all buffers
        try {
            DataBufferRegistry.shutdown();
        } catch (Exception e) {
            log.warn("Error shutting down DataBufferRegistry: %s", e.getMessage());
        }
        
        try {
            bridgeServer.close();
        } catch (Exception e) {
            log.warn("Error closing bridge server: %s", e.getMessage());
        }
        try {
            flightServer.close();
        } catch (Exception e) {
            log.warn("Error closing flight server: %s", e.getMessage());
        }
        
        try {
            connectionPool.shutdown();
        } catch (Exception e) {
            log.warn("Error shutting down connection pool: %s", e.getMessage());
        }
        
        log.info("TrinoExportConnector shutdown complete");
    }
}
