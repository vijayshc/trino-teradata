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

import io.trino.spi.Page;
import io.trino.spi.type.Type;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import io.airlift.log.Logger;

/**
 * Global registry for buffering Arrow batches received from Teradata.
 * Each query has its own isolated buffer identified by QueryID.
 * 
 * MEMORY MANAGEMENT: All cleanup is deterministic and integrated into query lifecycle:
 * - deregisterQuery(): Called when PageSource closes (normal completion)
 * - cleanupOnFailure(): Called when JDBC execution fails (error path)
 * 
 * IMPORTANT: This registry is static but per-JVM. In a multi-worker Trino cluster,
 * each worker has its own isolated instance. The design ensures:
 * 1. Each worker registers its own buffer when PageSource is created
 * 2. Each worker receives data only from AMPs assigned to it (deterministic routing)
 * 3. EOS is detected when expected count is received AND all connections/signals complete
 */
public class DataBufferRegistry {
    private static final Logger log = Logger.get(DataBufferRegistry.class);
    private static final Map<String, QueryBuffer> queryBuffers = new ConcurrentHashMap<>();
    private static final Map<String, List<Type>> schemaRegistry = new ConcurrentHashMap<>();
    
    // Dynamic per-query token storage for security
    private static final Map<String, String> dynamicTokenRegistry = new ConcurrentHashMap<>();
    
    // Scheduler for short-lived EOS timing checks and timeout fallback.
    // Increased to 4 threads to prevent cross-query interference during EOS signaling.
    private static final ScheduledExecutorService eosScheduler = Executors.newScheduledThreadPool(4, r -> {
        Thread t = new Thread(r, "data-buffer-eos-scheduler");
        t.setDaemon(true);
        return t;
    });
    
    // Timeout for expected count broadcast - if not received within this time, use fallback EOS logic
    private static final long EXPECTED_COUNT_TIMEOUT_MS = 60000; // 60 seconds

    // Max wait for PageSource to register schema before AMP data arrives
    private static final long SCHEMA_READY_TIMEOUT_MS = 30_000L;
    
    // Configurable queue capacity (set from TrinoExportConfig during initialization)
    private static int bufferQueueCapacity = 100;  // Default value

    // Approximate bytes currently buffered across all queries (for memory reporting)
    private static final AtomicInteger globalBufferedPages = new AtomicInteger(0);
    
    // NOTE: No TTL-based cleanup - all cleanup is deterministic via deregisterQuery/cleanupOnFailure

    private static class QueryBuffer {
        final BlockingQueue<BatchContainer> queue;
        final AtomicInteger activeConnections = new AtomicInteger(0);
        volatile boolean eosSignaled = false;
        volatile boolean hadAnyConnections = false;
        final long createdAt = System.currentTimeMillis();
        volatile long lastActivityTime = System.currentTimeMillis();
        final AtomicInteger activeConsumers = new AtomicInteger(0);
        
        // Multi-split tracking
        final AtomicInteger totalFinishedConsumers = new AtomicInteger(0);
        volatile int expectedConsumers = 1;
        
        // Timeout tracking for expected count fallback
        volatile boolean timeoutFallbackScheduled = false;
        
        // DETERMINISTIC EOS: expected AMP data connections for this worker.
        // Primary completion signal is data-socket close after all pages are pushed
        // (push-before-decrement). Optional TERADATA_FINISHED control messages still count
        // but are no longer required, removing a second TCP round-trip per AMP.
        final AtomicInteger expectedTeradataSignals = new AtomicInteger(-1);
        final AtomicInteger receivedTeradataSignals = new AtomicInteger(0);
        
        // Track total connections opened and closed for sanity checks
        final AtomicInteger totalConnectionsOpened = new AtomicInteger(0);
        final AtomicInteger totalConnectionsClosed = new AtomicInteger(0);

        // Approximate buffered page count for this query
        final AtomicInteger bufferedPages = new AtomicInteger(0);
        
        // Error signaling for immediate failure propagation
        volatile Exception queryError = null;
        volatile String errorMessage = null;
        
        // Schema readiness synchronization
        volatile boolean schemaReady = false;

        QueryBuffer(int capacity, int expectedConsumers) {
            this.queue = new LinkedBlockingQueue<>(capacity);
            this.expectedConsumers = expectedConsumers;
        }

        /**
         * Update the last activity timestamp to prevent premature EOS.
         */
        void updateActivity() {
            this.lastActivityTime = System.currentTimeMillis();
        }

        /**
         * Check and signal end-of-stream.
         *
         * DETERMINISTIC EOS LOGIC:
         * - Coordinator broadcasts exact expected AMP connection count per worker
         * - Each AMP data connection pushes all pages then decrements (push-before-decrement)
         * - EOS when: expected is known AND active==0 AND closed &gt;= expected
         * - Optional TERADATA_FINISHED control messages are accepted but not required
         */
        void checkAndSignalEos(String queryId) {
            int markerCount = 0;
            
            synchronized (this) {
                if (eosSignaled) return;
                
                int expected = expectedTeradataSignals.get();
                int received = receivedTeradataSignals.get();
                int opened = totalConnectionsOpened.get();
                int closed = totalConnectionsClosed.get();
                int active = activeConnections.get();
                int consumers = activeConsumers.get();
                
                boolean shouldEos = false;
                
                if (expected >= 0) {
                    // DETERMINISTIC PATH: Expected count is known from coordinator broadcast
                    if (expected == 0) {
                        // No connections expected for this worker
                        if (consumers > 0 || hadAnyConnections) {
                            shouldEos = true;
                            log.debug("EOS for query %s - no connections expected (deterministic)", queryId);
                        } else {
                            // Wait for consumer registration
                            log.debug("EOS check for query %s: expected=0, waiting for consumer", queryId);
                            return;
                        }
                    } else {
                        // Connection-close is authoritative (data fully buffered before decrement).
                        // Treat closed count as sufficient; finished signals only help fallback paths.
                        int completed = Math.max(closed, received);
                        boolean allDone = (active == 0) && (completed >= expected);
                        
                        if (allDone) {
                            shouldEos = true;
                            log.debug("EOS for query %s - all %d connections completed (deterministic, closed=%d received=%d)", 
                                    queryId, expected, closed, received);
                        }
                    }
                } else {
                    // expected < 0 means expected count not yet received
                    // TIMEOUT FALLBACK: Schedule a timeout check if not already scheduled
                    if (!timeoutFallbackScheduled) {
                        timeoutFallbackScheduled = true;
                        final String qId = queryId;
                        eosScheduler.schedule(() -> {
                            checkTimeoutFallbackEos(qId);
                        }, EXPECTED_COUNT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                        log.debug("Scheduled timeout fallback EOS check for query %s in %d ms", queryId, EXPECTED_COUNT_TIMEOUT_MS);
                    }
                    // Still waiting for expected count broadcast
                }
                
                if (shouldEos) {
                    log.debug("Signaling EOS for query %s: expected=%d, opened=%d, closed=%d, received=%d, consumers=%d", 
                            queryId, expected, opened, closed, received, consumers);
                    eosSignaled = true;
                    // Use actual active consumers, not expectedConsumers which may be wrong in multi-worker setup
                    markerCount = Math.max(consumers, 1);
                } else if (log.isDebugEnabled()) {
                    log.debug("EOS check for query %s: not ready - expected=%d, opened=%d, closed=%d, active=%d, received=%d, consumers=%d",
                            queryId, expected, opened, closed, active, received, consumers);
                }
            }

            // Perform potentially blocking operations OUTSIDE the synchronized block
            if (markerCount > 0) {
                try {
                    for (int i = 0; i < markerCount; i++) {
                        queue.put(BatchContainer.endOfStream());
                    }
                    log.debug("Pushed %d EOS markers for query %s", markerCount, queryId);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        
        void forceSignalEos(String queryId) {
            int markerCount = 0;
            synchronized (this) {
                if (!eosSignaled) {
                    eosSignaled = true;
                    // Use actual active consumers, not expectedConsumers
                    markerCount = Math.max(activeConsumers.get(), 1);
                }
            }
            
            if (markerCount > 0) {
                try {
                    for (int i = 0; i < markerCount; i++) {
                        queue.put(BatchContainer.endOfStream());
                    }
                    log.debug("Force signaled end of stream for query %s (pushed %d EOS markers)", queryId, markerCount);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        
        /**
         * Timeout fallback EOS check.
         * Called after EXPECTED_COUNT_TIMEOUT_MS if expected count was never received.
         * Uses connection-based heuristic: if activeConnections==0 and we had some connections,
         * assume all data has been received and signal EOS.
         */
        void checkTimeoutFallbackEos(String queryId) {
            int markerCount = 0;
            synchronized (this) {
                if (eosSignaled) return;
                
                int expected = expectedTeradataSignals.get();
                if (expected >= 0) {
                    // Expected count was received in the meantime, no need for fallback
                    return;
                }
                
                int opened = totalConnectionsOpened.get();
                int closed = totalConnectionsClosed.get();
                int active = activeConnections.get();
                int received = receivedTeradataSignals.get();
                int consumers = activeConsumers.get();
                
                // FALLBACK LOGIC: If we had connections and they're all closed, signal EOS
                // This handles the case where expected count broadcast failed
                if (hadAnyConnections && active == 0 && closed > 0) {
                    log.warn("TIMEOUT FALLBACK EOS for query %s: expected count never received, " +
                            "using connection-based heuristic (opened=%d, closed=%d, received=%d)",
                            queryId, opened, closed, received);
                    eosSignaled = true;
                    markerCount = Math.max(consumers, 1);
                } else if (!hadAnyConnections && consumers > 0) {
                    // No connections ever came, and consumer is waiting - likely 0-row result
                    log.warn("TIMEOUT FALLBACK EOS for query %s: no connections received, assuming 0-row result",
                            queryId);
                    eosSignaled = true;
                    markerCount = Math.max(consumers, 1);
                } else {
                    log.warn("TIMEOUT FALLBACK: Query %s still waiting (hadConnections=%b, active=%d, closed=%d, received=%d, consumers=%d)",
                            queryId, hadAnyConnections, active, closed, received, consumers);
                }
            }
            
            if (markerCount > 0) {
                try {
                    for (int i = 0; i < markerCount; i++) {
                        queue.put(BatchContainer.endOfStream());
                    }
                    log.info("Pushed %d EOS markers via timeout fallback for query %s", markerCount, queryId);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        void incrementConsumer(String queryId) {
            boolean pushMarker;
            boolean triggerEosCheck = false;
            synchronized (this) {
                activeConsumers.incrementAndGet();
                // If EOS was already signaled, this new consumer might have missed the original marker push
                pushMarker = eosSignaled;
                // Trigger EOS check if expected count is known (deterministic path)
                if (!eosSignaled && expectedTeradataSignals.get() >= 0) {
                    triggerEosCheck = true;
                }
            }
            
            if (pushMarker) {
                try {
                    queue.put(BatchContainer.endOfStream());
                    log.debug("Late consumer registered after EOS signaled - pushed immediate EOS marker");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            } else if (triggerEosCheck) {
                // Consumer registered and expected count known - check if we should signal EOS
                checkAndSignalEos(queryId);
            }
        }

        synchronized boolean deregisterConsumer() {
            activeConsumers.decrementAndGet();
            int finished = totalFinishedConsumers.incrementAndGet();

            // Return true if we should fully clean up this query
            return finished >= expectedConsumers;
        }
    }

    public static void setBufferQueueCapacity(int capacity) {
        bufferQueueCapacity = capacity;
        log.debug("DataBufferRegistry queue capacity set to %d", capacity);
    }

    public static int getBufferQueueCapacity() {
        return bufferQueueCapacity;
    }

    public static void registerQuery(String queryId) {
        registerQuery(queryId, 1);
    }

    public static void registerQuery(String queryId, int expectedConsumers) {
        queryBuffers.compute(queryId, (k, v) -> {
            if (v == null) {
                log.debug("Registered buffer for query %s (capacity: %d, consumers: %d)", queryId, bufferQueueCapacity, expectedConsumers);
                return new QueryBuffer(bufferQueueCapacity, expectedConsumers);
            } else {
                // Buffer exists, update expectation just in case
                v.expectedConsumers = Math.max(v.expectedConsumers, expectedConsumers);
                return v;
            }
        });
    }

    public static void incrementConsumers(String queryId) {
        QueryBuffer buffer = queryBuffers.get(queryId);
        if (buffer != null) {
            buffer.incrementConsumer(queryId);
        }
    }

    public static void deregisterQuery(String queryId) {
        QueryBuffer buffer = queryBuffers.get(queryId);
        if (buffer != null) {
            if (!buffer.deregisterConsumer()) {
                log.debug("Consumer closed for query %s. %d/%d finished.",
                        queryId, buffer.totalFinishedConsumers.get(), buffer.expectedConsumers);
                return;
            }

            queryBuffers.remove(queryId);
            // Also clean up schema registry, token registry, and performance profiler entries
            schemaRegistry.remove(queryId);
            dynamicTokenRegistry.remove(queryId);
            PerformanceProfiler.clear(queryId);

            log.debug("Deregistered buffer, schema, and profiler for query %s. All %d consumers finished.",
                    queryId, buffer.totalFinishedConsumers.get());
            while (!buffer.queue.isEmpty()) {
                BatchContainer container = buffer.queue.poll();
                if (container != null && !container.isEndOfStream()) {
                    try {
                        // Page doesn't need explicit closing like Arrow
                        container = null;
                    }
                    catch (Exception e) {
                        log.warn("Error during cleanup for query %s: %s", queryId, e.getMessage());
                    }
                }
            }
        }
    }
    
    // NOTE: cleanupStaleBuffers removed - all cleanup is deterministic via deregisterQuery/cleanupOnFailure

    public static BlockingQueue<BatchContainer> getBuffer(String queryId) {
        QueryBuffer buffer = queryBuffers.get(queryId);
        return buffer != null ? buffer.queue : null;
    }
    
    public static BlockingQueue<BatchContainer> getOrCreateBuffer(String queryId) {
        registerQuery(queryId);
        return getBuffer(queryId);
    }

    public static void incrementConnections(String queryId) {
        registerQuery(queryId);
        QueryBuffer buffer = queryBuffers.get(queryId);
        if (buffer != null) {
            buffer.updateActivity();
            buffer.hadAnyConnections = true;
            int active = buffer.activeConnections.incrementAndGet();
            int opened = buffer.totalConnectionsOpened.incrementAndGet();
            log.debug("Incremented connections for query %s: active=%d, totalOpened=%d", queryId, active, opened);
        }
    }

    public static void decrementConnections(String queryId) {
        QueryBuffer buffer = queryBuffers.get(queryId);
        if (buffer != null) {
            int active = buffer.activeConnections.decrementAndGet();
            int closed = buffer.totalConnectionsClosed.incrementAndGet();
            log.debug("Decremented connections for query %s: active=%d, totalClosed=%d", queryId, active, closed);
            buffer.checkAndSignalEos(queryId);
        }
    }

    /**
     * Mark that one Teradata AMP has finished its work (sent from UDF side).
     * With deterministic routing, each worker receives signals from specific AMPs.
     * 
     * CRITICAL: Signals can arrive BEFORE the buffer is registered on a worker (race condition).
     * We MUST create the buffer on-demand to avoid dropping signals.
     */
    public static void signalTeradataFinished(String queryId) {
        QueryBuffer buffer = queryBuffers.get(queryId);
        if (buffer == null) {
            // Signal arrived before buffer was registered - create it now to avoid losing the signal
            log.debug("Creating buffer on-demand for TERADATA_FINISHED signal (query %s)", queryId);
            registerQuery(queryId);
            buffer = queryBuffers.get(queryId);
        }
        if (buffer != null) {
            int received = buffer.receivedTeradataSignals.incrementAndGet();
            int expected = buffer.expectedTeradataSignals.get();
            log.debug("Teradata execution finished signal received for query %s (received=%d, expected=%d)", queryId, received, expected);
            buffer.updateActivity();
            buffer.checkAndSignalEos(queryId);
        }
    }
    
    /**
     * Set the expected number of TERADATA_FINISHED signals.
     * With deterministic routing, this is the exact count calculated by coordinator
     * using amp_id % num_workers for this specific worker.
     */
    public static void setExpectedTeradataSignals(String queryId, int count) {
        QueryBuffer buffer = queryBuffers.get(queryId);
        if (buffer == null) {
            registerQuery(queryId);
            buffer = queryBuffers.get(queryId);
        }
        if (buffer != null) {
            buffer.expectedTeradataSignals.set(count);
            log.debug("Set expectedTeradataSignals=%d for query %s", count, queryId);
            buffer.checkAndSignalEos(queryId);
        }
    }

    /**
     * PROACTIVE CLEANUP: Called when JDBC execution fails.
     * This cleans up immediately instead of waiting for TTL-based cleanup.
     * This prevents memory accumulation when queries fail before any data flows.
     * 
     * Only cleans up if there are no active consumers (PageSources) using the buffer.
     * If there are consumers, they will clean up when they close.
     */
    public static void cleanupOnFailure(String queryId) {
        QueryBuffer buffer = queryBuffers.get(queryId);
        if (buffer == null) {
            // Buffer was never created or already cleaned up
            return;
        }
        
        // Only clean up if no consumers are using this buffer
        // If consumers exist, let them clean up via deregisterQuery on close()
        if (buffer.activeConsumers.get() > 0) {
            log.debug("Skipping immediate cleanup for query %s: %d active consumers will clean up on close", 
                    queryId, buffer.activeConsumers.get());
            return;
        }
        
        // No consumers - proactively clean up now
        log.warn("Proactive cleanup on failure for query %s (no active consumers)", queryId);
        
        // Signal EOS to unblock any potential late joiners or waiting threads
        buffer.forceSignalEos(queryId);
        
        // Remove from all registries
        queryBuffers.remove(queryId);
        schemaRegistry.remove(queryId);
        dynamicTokenRegistry.remove(queryId);
        PerformanceProfiler.clear(queryId);
        
        // Clear the queue AFTER signaling EOS
        buffer.queue.clear();
    }
    
    public static boolean hasBuffer(String queryId) {
        return queryBuffers.containsKey(queryId);
    }

    public static void registerSchema(String queryId, List<Type> types) {
        registerQuery(queryId);
        schemaRegistry.put(queryId, types);
        QueryBuffer buffer = queryBuffers.get(queryId);
        if (buffer != null) {
            synchronized (buffer) {
                buffer.schemaReady = true;
                buffer.notifyAll();
            }
        }
        log.debug("Registered schema types for query %s: %s", queryId, types);
    }

    public static boolean awaitSchemaReady(String queryId) {
        QueryBuffer buffer = queryBuffers.get(queryId);
        if (buffer == null) {
            registerQuery(queryId);
            buffer = queryBuffers.get(queryId);
        }
        if (buffer == null) {
            return false;
        }
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(SCHEMA_READY_TIMEOUT_MS);
        synchronized (buffer) {
            while (!buffer.schemaReady) {
                if (!hasDynamicToken(queryId)) {
                    return false;
                }
                long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
                if (remainingMs <= 0) {
                    log.warn("Timed out waiting for schema ready for query %s after %d ms", queryId, SCHEMA_READY_TIMEOUT_MS);
                    return false;
                }
                try {
                    buffer.wait(Math.min(remainingMs, 1000L));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        }
        return true;
    }

    public static List<Type> getSchema(String queryId) {
        return schemaRegistry.get(queryId);
    }

    public static void pushData(String queryId, Page page) {
        registerQuery(queryId);
        QueryBuffer buffer = queryBuffers.get(queryId);
        if (buffer != null) {
            buffer.updateActivity();
            try {
                buffer.queue.put(BatchContainer.of(page));
                buffer.bufferedPages.incrementAndGet();
                globalBufferedPages.incrementAndGet();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Poll a page container and update buffered-page accounting.
     */
    public static BatchContainer pollData(String queryId, long timeoutMs) throws InterruptedException {
        QueryBuffer buffer = queryBuffers.get(queryId);
        if (buffer == null) {
            return null;
        }
        BatchContainer container = buffer.queue.poll(timeoutMs, TimeUnit.MILLISECONDS);
        if (container != null && !container.isEndOfStream()) {
            buffer.bufferedPages.decrementAndGet();
            globalBufferedPages.decrementAndGet();
        }
        return container;
    }

    public static long estimateBufferedBytes(String queryId) {
        QueryBuffer buffer = queryBuffers.get(queryId);
        if (buffer == null) {
            return 0;
        }
        // Rough estimate: assume average page ~256KB when present
        return Math.max(0, buffer.bufferedPages.get()) * 256L * 1024L;
    }

    public static int getGlobalBufferedPages() {
        return globalBufferedPages.get();
    }

    public static void pushEndMarker(String queryId) {
        QueryBuffer buffer = queryBuffers.get(queryId);
        if (buffer != null) {
            try {
                buffer.queue.put(BatchContainer.endOfStream());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Register a dynamic token for a query.
     * This token is generated per-query and used for socket authentication.
     */
    public static void registerDynamicToken(String queryId, String token) {
        dynamicTokenRegistry.put(queryId, token);
        log.debug("Registered dynamic token for query %s", queryId);
    }

    /**
     * Validate a dynamic token for a query.
     * @return true if the token matches the registered token for this query
     */
    public static boolean validateDynamicToken(String queryId, String receivedToken) {
        String expectedToken = dynamicTokenRegistry.get(queryId);
        if (expectedToken == null) {
            log.warn("No dynamic token registered for query %s", queryId);
            return false;
        }
        boolean valid = expectedToken.equals(receivedToken);
        if (!valid) {
            log.error("Invalid dynamic token for query %s", queryId);
        }
        return valid;
    }

    /**
     * Get the dynamic token for a query.
     */
    public static String getDynamicToken(String queryId) {
        return dynamicTokenRegistry.get(queryId);
    }

    /**
     * Check if a dynamic token is registered for a query.
     */
    public static boolean hasDynamicToken(String queryId) {
        return dynamicTokenRegistry.containsKey(queryId);
    }

    public static void shutdown() {
        log.debug("Shutting down DataBufferRegistry...");
        eosScheduler.shutdownNow();
        
        // Clean up all remaining buffers
        for (String queryId : queryBuffers.keySet()) {
            QueryBuffer buffer = queryBuffers.get(queryId);
            if (buffer != null) {
                buffer.forceSignalEos(queryId);
                buffer.queue.clear();
            }
        }
        queryBuffers.clear();
        schemaRegistry.clear();
        dynamicTokenRegistry.clear();
        log.debug("DataBufferRegistry shutdown complete. Cleared all buffers, schemas, and tokens.");
    }
}
