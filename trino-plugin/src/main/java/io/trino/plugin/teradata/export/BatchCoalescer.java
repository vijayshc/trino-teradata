package io.trino.plugin.teradata.export;

import io.airlift.log.Logger;
import io.trino.spi.Page;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Optimization #3: Batch Coalescing
 * 
 * Coalesces multiple small batches into larger ones before Page conversion
 * to reduce per-batch overhead (BlockBuilder allocation, queue operations).
 */
public final class BatchCoalescer {
    private static final Logger log = Logger.get(BatchCoalescer.class);

    private final int targetBatchSize;
    private final int maxWaitMs;
    
    // Accumulator for raw binary data
    private final List<byte[]> pendingBatches = new ArrayList<>();
    private final List<Integer> pendingLengths = new ArrayList<>();
    private int accumulatedRows = 0;

    public BatchCoalescer(int targetBatchSize, int maxWaitMs) {
        this.targetBatchSize = targetBatchSize;
        this.maxWaitMs = maxWaitMs;
    }

    /**
     * Add a batch to the coalescer.
     * @return Coalesced data if threshold reached, null otherwise
     */
    public synchronized CoalescedBatch addBatch(byte[] data, int length, int rowCount) {
        pendingBatches.add(data);
        pendingLengths.add(length);
        accumulatedRows += rowCount;

        if (accumulatedRows >= targetBatchSize) {
            return flush();
        }
        return null;
    }

    /**
     * Flush any pending batches.
     */
    public synchronized CoalescedBatch flush() {
        if (pendingBatches.isEmpty()) {
            return null;
        }

        // Calculate total size needed
        int totalSize = 4; // 4 bytes for total row count
        for (int i = 0; i < pendingBatches.size(); i++) {
            // Each batch has 4-byte row count at start, we skip those and add data
            totalSize += pendingLengths.get(i) - 4;
        }

        byte[] coalesced = new byte[totalSize];
        ByteBuffer buf = ByteBuffer.wrap(coalesced);
        buf.putInt(accumulatedRows);

        for (int i = 0; i < pendingBatches.size(); i++) {
            byte[] batch = pendingBatches.get(i);
            int len = pendingLengths.get(i);
            // Skip the first 4 bytes (row count) of each batch, copy the rest
            buf.put(batch, 4, len - 4);
        }

        CoalescedBatch result = new CoalescedBatch(coalesced, totalSize, accumulatedRows, pendingBatches.size());
        
        // Reset accumulator
        pendingBatches.clear();
        pendingLengths.clear();
        accumulatedRows = 0;

        return result;
    }

    public int getPendingRows() {
        return accumulatedRows;
    }

    public record CoalescedBatch(byte[] data, int length, int totalRows, int batchesCoalesced) {}
}
