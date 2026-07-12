package io.trino.plugin.teradata.export;

import io.airlift.log.Logger;
import io.trino.spi.Page;
import io.trino.spi.type.Type;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.Inflater;
import io.airlift.compress.lz4.Lz4Decompressor;
import java.util.zip.DataFormatException;

/**
 * High-performance async decompression pipeline.
 * Option D: Asynchronous Decompression Pipeline
 * 
 * Architecture:
 * [Network Reader] -> CompressedQueue -> [Decompression Pool] -> DecompressedQueue -> [Parser Pool] -> PageQueue
 */
public final class AsyncDecompressionPipeline {
    private static final Logger log = Logger.get(AsyncDecompressionPipeline.class);

    private final String queryId;
    private final List<DirectTrinoPageParser.ColumnSpec> columns;
    private final int compressionType; // 0=None, 1=ZLIB, 2=LZ4
    private final ByteBufferPool bufferPool;

    // Queues for async pipeline
    private final BlockingQueue<CompressedBatch> compressedQueue;

    // Thread pools
    private final ExecutorService decompressionPool;
    private final AtomicLong compressedBytes = new AtomicLong(0);
    private final AtomicLong decompressedBytes = new AtomicLong(0);
    private final AtomicLong totalRows = new AtomicLong(0);

    private volatile boolean finished = false;
    private volatile Throwable error = null;

    // Note: Each worker creates its own Inflater instance for thread safety and cleanup guarantee
    // No ThreadLocal pattern - ensures cleanup even on thread interruption
            
    private final CountDownLatch workerLatch;

    public record CompressedBatch(byte[] data, int length, boolean isEndMarker) {}

    public AsyncDecompressionPipeline(
            String queryId,
            List<DirectTrinoPageParser.ColumnSpec> columns,
            int compressionType,
            int queueCapacity,
            int decompressorThreads) {

        this.queryId = queryId;
        this.columns = columns;
        this.compressionType = compressionType;
        this.compressedQueue = new ArrayBlockingQueue<>(queueCapacity);
        this.bufferPool = new ByteBufferPool(decompressorThreads * 2, 64 * 1024 * 1024);
        this.workerLatch = new CountDownLatch(decompressorThreads);
        
        this.decompressionPool = Executors.newFixedThreadPool(decompressorThreads, r -> {
            Thread t = new Thread(r, "async-decompress-" + queryId.substring(Math.max(0, queryId.length() - 8)));
            t.setDaemon(true);
            return t;
        });

        // Start decompression workers
        for (int i = 0; i < decompressorThreads; i++) {
            decompressionPool.submit(this::decompressAndParseWorker);
        }
    }

    /**
     * Submit compressed batch for async processing.
     * Called by network reader thread.
     */
    public void submitCompressedBatch(byte[] data, int length) throws InterruptedException {
        compressedQueue.put(new CompressedBatch(data, length, false));
        compressedBytes.addAndGet(length);
    }

    /**
     * Signal end of data stream.
     */
    public void signalEndOfStream() throws InterruptedException {
        compressedQueue.put(new CompressedBatch(null, 0, true));
    }

    public boolean isFinished() {
        return finished;
    }

    public Throwable getError() {
        return error;
    }

    public long getCompressedBytes() {
        return compressedBytes.get();
    }

    public long getDecompressedBytes() {
        return decompressedBytes.get();
    }

    public long getTotalRows() {
        return totalRows.get();
    }

    private void decompressAndParseWorker() {
        byte[] decompressionBuffer = new byte[64 * 1024 * 1024];
        Inflater inflater = null;  // Per-worker instance, not ThreadLocal
        
        try {
            // Create own Inflater instance for this worker
            inflater = (compressionType == 1) ? new Inflater() : null;
            Lz4Decompressor lz4Decompressor = (compressionType == 2) ? new Lz4Decompressor() : null;
            
            while (!finished) {
                CompressedBatch batch = compressedQueue.poll(100, TimeUnit.MILLISECONDS);
                if (batch == null) continue;

                if (batch.isEndMarker()) {
                    finished = true;
                    // Push end marker through for other workers
                    compressedQueue.offer(batch);
                    break;
                }

                byte[] decompressed;
                int decompressedLen;

                if (compressionType == 1) { /* ZLIB */
                    inflater.reset();
                    inflater.setInput(batch.data, 0, batch.length);
                    decompressedLen = inflater.inflate(decompressionBuffer);
                    decompressed = decompressionBuffer;
                } else if (compressionType == 2) { /* LZ4 */
                    // LZ4 decompression requires destination size or we can use the aircompressor version
                    // The aircompressor LZ4Decompressor.decompress(input, inputOffset, inputLength, output, outputOffset, maxOutputLength)
                    decompressedLen = lz4Decompressor.decompress(batch.data, 0, batch.length, decompressionBuffer, 0, decompressionBuffer.length);
                    decompressed = decompressionBuffer;
                } else { /* NONE */
                    decompressed = batch.data;
                    decompressedLen = batch.length;
                }

                decompressedBytes.addAndGet(decompressedLen);

                // Parse directly to Trino Page
                Page page = DirectTrinoPageParser.parseDirectToPage(decompressed, decompressedLen, columns);
                if (page != null) {
                    int rowCount = page.getPositionCount();
                    totalRows.addAndGet(rowCount);
                    if (rowCount > 0) {
                        DataBufferRegistry.pushData(queryId, page);
                        log.debug("Pushed page with %d rows for query %s", rowCount, queryId);
                    } else {
                        log.debug("Skipping empty page (0 rows) for query %s. Decompressed length: %d", queryId, decompressedLen);
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.debug("Decompression worker interrupted for query %s", queryId);
        } catch (Exception e) {
            error = e;
            log.error(e, "Error in async decompression worker for query %s", queryId);
        } finally {
            // CRITICAL: Clean up Inflater native resources - guaranteed even on interrupt
            if (inflater != null) {
                try {
                    inflater.end();
                } catch (Exception e) {
                    log.warn("Error ending Inflater in worker: %s", e.getMessage());
                }
            }
            workerLatch.countDown();
        }
    }

    public void shutdown() {
        finished = true;
        decompressionPool.shutdownNow();
        // CRITICAL: Release DirectByteBuffer native memory
        if (bufferPool != null) {
            bufferPool.close();
        }
    }
    
    public void awaitCompletion() throws InterruptedException {
        workerLatch.await();
    }

    /**
     * Parse column specs from schema JSON.
     */
    public static List<DirectTrinoPageParser.ColumnSpec> parseSchema(String json, List<Type> trinoTypes) {
        List<DirectTrinoPageParser.ColumnSpec> columns = new ArrayList<>();
        int columnsStart = json.indexOf("[");
        int columnsEnd = json.lastIndexOf("]");
        if (columnsStart < 0 || columnsEnd < 0) return columns;

        String columnsJson = json.substring(columnsStart + 1, columnsEnd);
        int pos = 0;
        int typeIdx = 0;
        while (pos < columnsJson.length()) {
            int objStart = columnsJson.indexOf("{", pos);
            if (objStart < 0) break;
            int objEnd = columnsJson.indexOf("}", objStart);
            if (objEnd < 0) break;
            String colJson = columnsJson.substring(objStart, objEnd + 1);
            String name = extractJsonString(colJson, "name");
            String type = extractJsonString(colJson, "type");
            if (name != null && type != null && typeIdx < trinoTypes.size()) {
                columns.add(new DirectTrinoPageParser.ColumnSpec(name, type, trinoTypes.get(typeIdx)));
                typeIdx++;
            }
            pos = objEnd + 1;
        }
        return columns;
    }

    private static String extractJsonString(String json, String key) {
        String searchKey = "\"" + key + "\":\"";
        int start = json.indexOf(searchKey);
        if (start < 0) return null;
        start += searchKey.length();
        int end = json.indexOf("\"", start);
        if (end < 0) return null;
        return json.substring(start, end);
    }
}
