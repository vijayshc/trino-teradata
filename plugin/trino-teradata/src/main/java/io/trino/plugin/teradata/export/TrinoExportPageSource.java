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
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Page source that receives data from Teradata via the Bridge Server.
 * 
 * Handles column reordering when SQL order differs from Trino expected order.
 */
public class TrinoExportPageSource implements ConnectorPageSource {
    private static final Logger log = Logger.get(TrinoExportPageSource.class);
    
    private final String queryId;
    private final BlockingQueue<BatchContainer> buffer;
    private final List<JdbcColumnHandle> sqlOrderColumns;     // Order of columns in SQL (matches binary data)
    private final List<JdbcColumnHandle> trinoExpectedColumns; // Order Trino expects in output
    private final int[] columnReorderMap;  // Maps from Trino expected position to SQL position
    private final boolean needsReordering;
    private final java.time.ZoneOffset teradataZoneOffset;
    private final java.time.ZoneOffset localZoneOffset;
    private final long pagePollTimeoutMs;
    private final boolean enableDebugLogging;
    private long completedBytes = 0;
    private boolean finished = false;

    public TrinoExportPageSource(String queryId, 
                                  List<ColumnHandle> sqlOrderColumns, 
                                  List<ColumnHandle> trinoExpectedColumns,
                                  String teradataTimezone, 
                                  long pagePollTimeoutMs, 
                                  boolean enableDebugLogging, 
                                  int expectedConsumers, 
                                  String token) {
        this.queryId = queryId;
        
        try {
            // CRITICAL: Register buffer on THIS worker (multi-worker support)
            DataBufferRegistry.registerQuery(queryId, expectedConsumers);
            
            // Register the security token on THIS worker
            if (token != null) {
                DataBufferRegistry.registerDynamicToken(queryId, token);
            }
            
            this.buffer = DataBufferRegistry.getBuffer(queryId);
            
            // Register this instance as a consumer to support parallel split processing
            DataBufferRegistry.incrementConsumers(queryId);
            
            this.sqlOrderColumns = sqlOrderColumns.stream()
                    .map(JdbcColumnHandle.class::cast)
                    .collect(Collectors.toList());
            
            this.trinoExpectedColumns = trinoExpectedColumns.stream()
                    .map(JdbcColumnHandle.class::cast)
                    .collect(Collectors.toList());
    
            // CRITICAL: Register Schema Types using SQL order (matches binary data from Teradata)
            List<io.trino.spi.type.Type> types = this.sqlOrderColumns.stream()
                    .map(JdbcColumnHandle::getColumnType)
                    .collect(Collectors.toList());
            DataBufferRegistry.registerSchema(queryId, types);

            
            // Build reorder map: columnReorderMap[trinoPos] = sqlPos
            // This tells us where to find each column Trino expects in the SQL-ordered data
            this.columnReorderMap = buildReorderMap(this.sqlOrderColumns, this.trinoExpectedColumns);
            this.needsReordering = checkNeedsReordering(this.columnReorderMap);
            
            if (this.needsReordering) {
                log.debug("Query %s needs column reordering. SQL order: %s, Trino expects: %s", 
                        queryId,
                        this.sqlOrderColumns.stream().map(JdbcColumnHandle::getColumnName).collect(Collectors.toList()),
                        this.trinoExpectedColumns.stream().map(JdbcColumnHandle::getColumnName).collect(Collectors.toList()));
            }
            
            this.pagePollTimeoutMs = pagePollTimeoutMs;
            this.enableDebugLogging = enableDebugLogging;
            
            // Parse Teradata timezone offset
            this.teradataZoneOffset = java.time.ZoneOffset.of(teradataTimezone);
            // Get local timezone offset
            this.localZoneOffset = java.time.ZoneId.systemDefault().getRules().getOffset(java.time.Instant.now());
            
            log.debug("PageSource created for query %s. Registered %d column types. Needs reordering: %s", 
                    queryId, types.size(), needsReordering);
        } catch (Throwable t) {
            // Memory Leak Prevention: If constructor fails, we must clean up the registry
            // otherwise the query entry persists forever as close() will never be called
            log.error(t, "Failed to initialize PageSource for %s - cleaning up registry", queryId);
            try {
                DataBufferRegistry.deregisterQuery(queryId);
            } catch (Exception e) {
                log.warn(e, "Error during cleanup after constructor failure for %s", queryId);
            }
            throw t;
        }
    }

    private int[] buildReorderMap(List<JdbcColumnHandle> sqlOrder, List<JdbcColumnHandle> trinoOrder) {
        int[] map = new int[trinoOrder.size()];
        
        for (int trinoPos = 0; trinoPos < trinoOrder.size(); trinoPos++) {
            JdbcColumnHandle expected = trinoOrder.get(trinoPos);
            int sqlPos = -1;
            
            // Find the matching column in SQL order by column name
            for (int i = 0; i < sqlOrder.size(); i++) {
                if (sqlOrder.get(i).getColumnName().equals(expected.getColumnName())) {
                    sqlPos = i;
                    break;
                }
            }
            
            if (sqlPos == -1) {
                // Fallback: use same position if names don't match
                sqlPos = trinoPos < sqlOrder.size() ? trinoPos : 0;
                log.warn("Could not find SQL column for Trino expected column %s at position %d. Using position %d",
                        expected.getColumnName(), trinoPos, sqlPos);
            }
            
            map[trinoPos] = sqlPos;
        }
        
        return map;
    }
    
    private boolean checkNeedsReordering(int[] map) {
        for (int i = 0; i < map.length; i++) {
            if (map[i] != i) {
                return true;
            }
        }
        return false;
    }
    
    private Page reorderPage(Page page) {
        if (!needsReordering || page == null) {
            return page;
        }
        
        int positionCount = page.getPositionCount();
        Block[] reorderedBlocks = new Block[trinoExpectedColumns.size()];
        
        for (int trinoPos = 0; trinoPos < trinoExpectedColumns.size(); trinoPos++) {
            int sqlPos = columnReorderMap[trinoPos];
            if (sqlPos < page.getChannelCount()) {
                reorderedBlocks[trinoPos] = page.getBlock(sqlPos);
            } else {
                log.error("Invalid column mapping: trinoPos=%d -> sqlPos=%d, but page only has %d channels",
                        trinoPos, sqlPos, page.getChannelCount());
                return page;  // Return original page on error
            }
        }
        
        return new Page(positionCount, reorderedBlocks);
    }

    @Override
    public long getCompletedBytes() {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public boolean isFinished() {
        return finished;
    }

    @Override
    public SourcePage getNextSourcePage() {
        if (finished) {
            return null;
        }

        try {
            // Profile: Queue Poll
            long pollStart = System.nanoTime();
            BatchContainer container = buffer.poll(pagePollTimeoutMs, TimeUnit.MILLISECONDS);
            long pollEnd = System.nanoTime();
            boolean waited = container == null || (pollEnd - pollStart) > 1_000_000;
            PerformanceProfiler.recordQueuePoll(queryId, pollEnd - pollStart, waited);
            
            if (container == null) {
                return null;
            }

            if (container.isEndOfStream()) {
                log.debug("Received end of stream for query %s", queryId);
                finished = true;
                PerformanceProfiler.generateSummary(queryId);
                return null;
            }

            Page page = container.page();
            
            if (page != null) {
                // Reorder columns if needed to match Trino expected order
                page = reorderPage(page);
                completedBytes += page.getSizeInBytes();
                return SourcePage.create(page);
            }
            return null;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            finished = true;
            return null;
        }

    }

    @Override
    public long getMemoryUsage() {
        return 0;
    }

    @Override
    public void close() throws IOException {
        finished = true;
        try {
            DataBufferRegistry.deregisterQuery(queryId);
        } catch (Exception e) {
            log.warn("Error during cleanup for query %s: %s", queryId, e.getMessage());
        }
    }
}
