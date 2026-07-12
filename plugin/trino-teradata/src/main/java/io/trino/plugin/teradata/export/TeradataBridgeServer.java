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
import io.trino.spi.type.Type;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import com.google.inject.Inject;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * High-performance Java Bridge Server that receives data directly from Teradata AMPs.
 * 
 * Optimized Architecture:
 * - Uses AsyncDecompressionPipeline for parallel decompression and parsing.
 * - Uses DirectTrinoPageParser for zero-copy binary-to-page conversion.
 * - Eliminates Apache Arrow to remove overhead.
 */
public class TeradataBridgeServer implements AutoCloseable {
    private static final Logger log = Logger.get(TeradataBridgeServer.class);
    
    // Magic number for control messages
    public static final int CONTROL_MAGIC = 0xFEEDFACE;
    
    private final int port;
    private final int socketReceiveBufferSize;
    private final int inputBufferSize;
    private final ExecutorService executor;
    private ServerSocket serverSocket;
    private final TrinoExportConfig config;
    private volatile boolean running = true;

    @Inject
    public TeradataBridgeServer(TrinoExportConfig config) {
        this.config = config;
        this.port = config.getBridgePort();
        this.socketReceiveBufferSize = config.getSocketReceiveBufferSize();
        this.inputBufferSize = config.getInputBufferSize();
        
        // Use bounded thread pool to prevent memory exhaustion
        int coreThreads = config.getBridgeCorePoolSize();
        int maxThreads = config.getMaxBridgeThreads();
        int queueCapacity = config.getBridgeQueueCapacity();
        this.executor = new ThreadPoolExecutor(
                coreThreads,
                maxThreads,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(queueCapacity),
                r -> {
                    Thread t = new Thread(r, "teradata-bridge-handler");
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.CallerRunsPolicy()  // Back-pressure: caller handles if queue full
        );
        
        log.debug("TeradataBridgeServer initialized with socketReceiveBufferSize=%d, inputBufferSize=%d, coreThreads=%d, maxThreads=%d, queueCapacity=%d",
                socketReceiveBufferSize, inputBufferSize, coreThreads, maxThreads, queueCapacity);
    }

    @PostConstruct
    public void start() {
        executor.submit(this::runServer);
        log.debug("Teradata Bridge Server starting on port %d", port);
    }

    private void runServer() {
        try {
            serverSocket = new ServerSocket(port);
            log.debug("Teradata Bridge Server listening on port %d", port);
            
            while (running) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    clientSocket.setTcpNoDelay(true);
                    clientSocket.setReceiveBufferSize(socketReceiveBufferSize);
                    clientSocket.setSoTimeout(config.getBridgeSocketTimeoutMs());
                    log.debug("Connection from %s", clientSocket.getRemoteSocketAddress());
                    executor.submit(() -> handleClient(clientSocket));
                } catch (IOException e) {
                    if (running) {
                        log.warn("Error accepting connection: %s", e.getMessage());
                    }
                }
            }
        } catch (IOException e) {
            log.error(e, "Failed to start bridge server on port %d", port);
        }
    }

    private void handleClient(Socket socket) {
        String queryId = "unknown";
        boolean incremented = false;
        long compressedBytes = 0;
        long decompressedBytes = 0;
        int totalRows = 0;
        java.util.zip.Inflater inflater = null;  // Declare outside try for proper cleanup
            
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), inputBufferSize));
             DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {
            
            // 1. Mandatory Dynamic Token Validation
            // Protocol: [tokenLen (int)][token (string)][queryIdLen/Magic (int)][queryId (string)]...
            int tokenLen = in.readInt();
            if (tokenLen <= 0 || tokenLen > 1024) {
                log.error("Invalid token length: %d from %s", tokenLen, socket.getRemoteSocketAddress());
                return;
            }
            byte[] tokenBytes = new byte[tokenLen];
            in.readFully(tokenBytes);
            String receivedToken = new String(tokenBytes, StandardCharsets.UTF_8);
            
            // 2. Read Magic Number or Query ID Length
            int lenOrMagic = in.readInt();
            
            if (lenOrMagic == CONTROL_MAGIC) {
                handleControlMessage(in, out, receivedToken);
                return;
            }
            
            // It's a normal Query ID - Validate length to prevent NegativeArraySizeException
            if (lenOrMagic <= 0 || lenOrMagic > 1024) {
                log.error("Invalid Query ID length or Magic Number: %d from %s", lenOrMagic, socket.getRemoteSocketAddress());
                return;
            }
            byte[] queryIdBytes = new byte[lenOrMagic];
            in.readFully(queryIdBytes);
            queryId = new String(queryIdBytes, StandardCharsets.UTF_8);
            
            // Now we have both QueryId and Token - Validate it
            if (!DataBufferRegistry.validateDynamicToken(queryId, receivedToken)) {
                log.error("Unauthorized: Invalid dynamic token for query %s from %s", queryId, socket.getRemoteSocketAddress());
                out.write("ERROR: UNAUTHORIZED".getBytes(StandardCharsets.UTF_8));
                out.flush();
                return;
            }

            log.debug("Receiving data for authenticated query: %s", queryId);

            // Register this connection FIRST (before any data processing)
            DataBufferRegistry.incrementConnections(queryId);
            incremented = true;
            
            // Read Compression Type
            int compressionType = in.readInt();
            String algo = (compressionType == 2) ? "LZ4" : (compressionType == 1) ? "ZLIB" : "NONE";
            if (compressionType != 0) {
                log.debug("AUTHENTICATION SUCCESS: Query %s using compression %s", queryId, algo);
            } else {
                log.debug("AUTHENTICATION SUCCESS: Query %s with compression DISABLED", queryId);
            }

            // Read Schema JSON (for verification and name matching)
            int schemaLen = in.readInt();
            byte[] schemaBytes = new byte[schemaLen];
            in.readFully(schemaBytes);
            String schemaJson = new String(schemaBytes, StandardCharsets.UTF_8);
            log.debug("Received schema JSON for query %s: %s", queryId, schemaJson);
            
            // Fetch registered Trino Types (Critical for direct parsing)
            if (!DataBufferRegistry.awaitSchemaReady(queryId)) {
                log.warn("Query %s schema not ready or query deregistered. Aborting connection.", queryId);
                return;
            }
            List<Type> trinoTypes = DataBufferRegistry.getSchema(queryId);
            if (trinoTypes == null) {
                throw new IllegalStateException("No Trino schema registered for query " + queryId + ". PageSource implementation must register schema before data transfer.");
            }
            
            // Create Column Specs using the existing helper method
            List<DirectTrinoPageParser.ColumnSpec> columns = AsyncDecompressionPipeline.parseSchema(schemaJson, trinoTypes);
            
            // Initialize profiler
            PerformanceProfiler.getOrCreate(queryId);
            
            // Synchronous processing - create decompression buffer with enough space for max Teradata batch (16MB)
            // Using 32MB to be absolutely safe and avoid reallocations
            inflater = (compressionType == 1) ? new java.util.zip.Inflater() : null;
            io.airlift.compress.lz4.Lz4Decompressor lz4Decompressor = (compressionType == 2) ? new io.airlift.compress.lz4.Lz4Decompressor() : null;
            byte[] decompressionBuffer = (compressionType != 0) ? new byte[32 * 1024 * 1024] : null;

            // Read and process batches synchronously until end of stream
            while (true) {
                // Profile: Network Read
                long netStart = System.nanoTime();
                int batchLen = in.readInt();
                if (batchLen == 0) {
                    log.debug("End of stream (marker) for query %s", queryId);
                    break;
                }
                
                byte[] batchData = new byte[batchLen];
                in.readFully(batchData);
                long netEnd = System.nanoTime();
                PerformanceProfiler.recordNetworkRead(queryId, netEnd - netStart, batchLen);
                compressedBytes += batchLen;
                
                // SYNCHRONOUS: Decompress immediately in this thread
                byte[] decompressed;
                int decompressedLen;
                
                if (compressionType == 1) { /* ZLIB */
                    long decompStart = System.nanoTime();
                    inflater.reset();
                    inflater.setInput(batchData, 0, batchLen);
                    
                    // Ensure buffer is large enough for decompression. 
                    // Max Teradata batch is 16MB, so 32MB should always be enough.
                    if (decompressionBuffer.length < 32 * 1024 * 1024) {
                        decompressionBuffer = new byte[32 * 1024 * 1024];
                    }
                    
                    decompressedLen = inflater.inflate(decompressionBuffer);
                    long decompEnd = System.nanoTime();
                    PerformanceProfiler.recordDecompression(queryId, decompEnd - decompStart, decompressedLen);
                    decompressed = decompressionBuffer;
                    decompressedBytes += decompressedLen;
                } else if (compressionType == 2) { /* LZ4 */
                    long decompStart = System.nanoTime();
                    // Ensure buffer is large enough.
                    if (decompressionBuffer.length < 32 * 1024 * 1024) {
                        decompressionBuffer = new byte[32 * 1024 * 1024];
                    }
                    decompressedLen = lz4Decompressor.decompress(batchData, 0, batchLen, decompressionBuffer, 0, decompressionBuffer.length);
                    long decompEnd = System.nanoTime();
                    PerformanceProfiler.recordDecompression(queryId, decompEnd - decompStart, decompressedLen);
                    decompressed = decompressionBuffer;
                    decompressedBytes += decompressedLen;
                } else {
                    decompressed = batchData;
                    decompressedLen = batchLen;
                    decompressedBytes += batchLen;
                }

                // SYNCHRONOUS: Parse directly to Trino Page in this thread
                long parseStart = System.nanoTime();
                io.trino.spi.Page page = DirectTrinoPageParser.parseDirectToPage(decompressed, decompressedLen, columns);
                long parseEnd = System.nanoTime();
                
                if (page != null && page.getPositionCount() > 0) {
                    totalRows += page.getPositionCount();
                    PerformanceProfiler.recordDirectParsing(queryId, parseEnd - parseStart, page.getPositionCount());
                    
                    // SYNCHRONOUS: Push to buffer in this thread
                    // Data is in buffer BEFORE we move to next batch or decrement connection
                    long pushStart = System.nanoTime();
                    DataBufferRegistry.pushData(queryId, page);
                    long pushEnd = System.nanoTime();
                    PerformanceProfiler.recordQueuePush(queryId, pushEnd - pushStart, (pushEnd - pushStart) > 1_000_000);
                }
            }
            
            // All data is now in the buffer - safe to send acknowledgment
            out.write("OK".getBytes(StandardCharsets.UTF_8));
            out.flush();
            
            double ratio = compressedBytes > 0 ? (double) decompressedBytes / compressedBytes : 1.0;
            log.debug("Successfully processed query %s: %d rows, %.2f MB compressed, %.2f MB decompressed (Ratio: %.2fx)", 
                queryId, totalRows, compressedBytes / (1024.0 * 1024.0), decompressedBytes / (1024.0 * 1024.0), ratio);
            
        } catch (EOFException e) {
            if ("unknown".equals(queryId)) {
                log.debug("Handshake failed or connection closed immediately (likely health check) from %s", socket.getRemoteSocketAddress());
            } else {
                log.error(e, "Connection lost unexpectedly for query %s", queryId);
            }
        } catch (Exception e) {
            log.error(e, "Error handling client for query %s", queryId);
        } finally {
            // CRITICAL: Release Inflater native memory to prevent native memory leak
            if (inflater != null) {
                try {
                    inflater.end();
                } catch (Exception e) {
                    log.warn("Error ending Inflater for query %s: %s", queryId, e.getMessage());
                }
            }
            
            // CRITICAL: Connection is only decremented AFTER all data is in the buffer
            if (incremented) {
                DataBufferRegistry.decrementConnections(queryId);
            }
            try {
                socket.close();
            } catch (IOException ignored) {}
        }
    }

    private void handleControlMessage(DataInputStream in, DataOutputStream out, String receivedToken) throws IOException {
        String queryId = "unknown";
        try {
            int qidLen = in.readInt();
            byte[] qidBytes = new byte[qidLen];
            in.readFully(qidBytes);
            queryId = new String(qidBytes, StandardCharsets.UTF_8);
            
            int command = in.readInt();
            log.debug("Received control message: command=%d for query %s", command, queryId);
            
            // === SECURITY: Validate token for all commands except REGISTER_TOKEN (5) ===
            // Command 5 is used to register the token itself, so it can't be validated
            if (command != 5) {
                if (!DataBufferRegistry.validateDynamicToken(queryId, receivedToken)) {
                    log.error("SECURITY: Unauthorized control message command=%d for query %s - invalid token", command, queryId);
                    out.write("ER".getBytes(StandardCharsets.UTF_8));
                    out.flush();
                    return;
                }
            }
            
            if (command == 1) { // 1 = TERADATA_FINISHED (from UDF)
                log.debug("Received External EOS signal from Teradata for query %s", queryId);
                DataBufferRegistry.signalTeradataFinished(queryId);
            } else if (command == 2) { // 2 = JDBC_FINISHED (deprecated - using deterministic EOS)
                log.debug("Ignoring deprecated JDBC_FINISHED signal for query %s (using deterministic EOS)", queryId);
                // No action needed - deterministic EOS uses expected count broadcast instead
            } else if (command == 3) { // 3 = EXPECTED_TERADATA_SIGNALS (per-worker expected count)
                int expectedSignals = in.readInt();
                log.debug("Received expected Teradata signals for query %s: %d", queryId, expectedSignals);
                DataBufferRegistry.setExpectedTeradataSignals(queryId, expectedSignals);
            } else if (command == 5) { // 5 = REGISTER_TOKEN (pre-register token before data connections)
                DataBufferRegistry.registerDynamicToken(queryId, receivedToken);
                log.debug("Registered dynamic token via control message for query %s", queryId);
            }
            
            out.write("OK".getBytes(StandardCharsets.UTF_8));
            out.flush();
        } catch (Exception e) {
            log.error(e, "Error handling control message for query %s", queryId);
        }
    }

    @Override
    @PreDestroy
    public void close() {
        running = false;
        try {
            if (serverSocket != null) serverSocket.close();
        } catch (IOException ignored) {}
        executor.shutdownNow();
        log.debug("Teradata Bridge Server stopped");
    }
}
