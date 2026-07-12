package io.trino.plugin.teradata.export;

import io.airlift.log.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Performance profiler for the Teradata Export Connector.
 * Tracks time spent in each phase of the data pipeline to identify bottlenecks.
 */
public final class PerformanceProfiler {
    private static final Logger log = Logger.get(PerformanceProfiler.class);

    // Per-query profiling data
    private static final ConcurrentHashMap<String, QueryProfile> profiles = new ConcurrentHashMap<>();

    public static class QueryProfile {
        public final String queryId;
        public final long startTimeMs;

        // Phase timings (nanoseconds for precision)
        public final AtomicLong networkReadNanos = new AtomicLong(0);
        public final AtomicLong decompressionNanos = new AtomicLong(0);
        public final AtomicLong directParsingNanos = new AtomicLong(0);
        public final AtomicLong queuePushNanos = new AtomicLong(0);
        public final AtomicLong queuePollNanos = new AtomicLong(0);

        // Counters
        public final AtomicLong batchCount = new AtomicLong(0);
        public final AtomicLong rowCount = new AtomicLong(0);
        public final AtomicLong compressedBytes = new AtomicLong(0);
        public final AtomicLong decompressedBytes = new AtomicLong(0);
        public final AtomicLong networkWaitCount = new AtomicLong(0);
        public final AtomicLong queueFullWaitCount = new AtomicLong(0);

        // First/last batch timestamps for throughput calculation
        public volatile long firstBatchTimeMs = 0;
        public volatile long lastBatchTimeMs = 0;

        public QueryProfile(String queryId) {
            this.queryId = queryId;
            this.startTimeMs = System.currentTimeMillis();
        }
    }

    public static QueryProfile getOrCreate(String queryId) {
        return profiles.computeIfAbsent(queryId, QueryProfile::new);
    }

    public static QueryProfile get(String queryId) {
        return profiles.get(queryId);
    }

    /**
     * Record time for a specific phase.
     */
    public static void recordNetworkRead(String queryId, long nanos, int bytes) {
        QueryProfile p = get(queryId);
        if (p != null) {
            p.networkReadNanos.addAndGet(nanos);
            p.compressedBytes.addAndGet(bytes);
        }
    }

    public static void recordDecompression(String queryId, long nanos, int decompressedBytes) {
        QueryProfile p = get(queryId);
        if (p != null) {
            p.decompressionNanos.addAndGet(nanos);
            p.decompressedBytes.addAndGet(decompressedBytes);
        }
    }

    public static void recordDirectParsing(String queryId, long nanos, int rows) {
        QueryProfile p = get(queryId);
        if (p != null) {
            p.directParsingNanos.addAndGet(nanos);
            p.rowCount.addAndGet(rows);
            p.batchCount.incrementAndGet();
            if (p.firstBatchTimeMs == 0) {
                p.firstBatchTimeMs = System.currentTimeMillis();
            }
            p.lastBatchTimeMs = System.currentTimeMillis();
        }
    }

    public static void recordQueuePush(String queryId, long nanos, boolean waited) {
        QueryProfile p = get(queryId);
        if (p != null) {
            p.queuePushNanos.addAndGet(nanos);
            if (waited) p.queueFullWaitCount.incrementAndGet();
        }
    }

    public static void recordQueuePoll(String queryId, long nanos, boolean waited) {
        QueryProfile p = get(queryId);
        if (p != null) {
            p.queuePollNanos.addAndGet(nanos);
            if (waited) p.networkWaitCount.incrementAndGet();
        }
    }



    /**
     * Generate and log a detailed profile summary for a query.
     */
    public static String generateSummary(String queryId) {
        QueryProfile p = profiles.remove(queryId);
        if (p == null) return "No profile data for query " + queryId;

        long totalTimeMs = System.currentTimeMillis() - p.startTimeMs;
        long dataTransferTimeMs = p.lastBatchTimeMs > 0 ? p.lastBatchTimeMs - p.firstBatchTimeMs : 0;

        // Convert nanos to millis
        double networkMs = p.networkReadNanos.get() / 1_000_000.0;
        double decompressMs = p.decompressionNanos.get() / 1_000_000.0;
        double directParseMs = p.directParsingNanos.get() / 1_000_000.0;
        double queuePushMs = p.queuePushNanos.get() / 1_000_000.0;
        double queuePollMs = p.queuePollNanos.get() / 1_000_000.0;

        double totalProfiledMs = networkMs + decompressMs + directParseMs + queuePushMs + queuePollMs;
        double overheadMs = totalTimeMs - totalProfiledMs;

        // Calculate throughput
        double compressedMB = p.compressedBytes.get() / (1024.0 * 1024.0);
        double decompressedMB = p.decompressedBytes.get() / (1024.0 * 1024.0);
        double throughputMBps = dataTransferTimeMs > 0 ? (decompressedMB / (dataTransferTimeMs / 1000.0)) : 0;
        double rowsPerSec = dataTransferTimeMs > 0 ? (p.rowCount.get() / (dataTransferTimeMs / 1000.0)) : 0;

        // Calculate percentages
        double networkPct = totalTimeMs > 0 ? (networkMs / totalTimeMs * 100) : 0;
        double decompressPct = totalTimeMs > 0 ? (decompressMs / totalTimeMs * 100) : 0;
        double directParsePct = totalTimeMs > 0 ? (directParseMs / totalTimeMs * 100) : 0;
        double queuePushPct = totalTimeMs > 0 ? (queuePushMs / totalTimeMs * 100) : 0;
        double queuePollPct = totalTimeMs > 0 ? (queuePollMs / totalTimeMs * 100) : 0;

        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append("╔══════════════════════════════════════════════════════════════════════════════╗\n");
        sb.append("║                    PERFORMANCE PROFILE SUMMARY                               ║\n");
        sb.append("╠══════════════════════════════════════════════════════════════════════════════╣\n");
        sb.append(String.format("║ Query ID: %-67s ║\n", queryId.substring(Math.max(0, queryId.length() - 50))));
        sb.append("╠══════════════════════════════════════════════════════════════════════════════╣\n");
        sb.append("║ DATA VOLUME                                                                  ║\n");
        sb.append(String.format("║   Total Rows:        %,15d    Batches:       %,10d           ║\n", p.rowCount.get(), p.batchCount.get()));
        sb.append(String.format("║   Compressed:        %,12.2f MB    Decompressed: %,12.2f MB        ║\n", compressedMB, decompressedMB));
        sb.append(String.format("║   Compression Ratio: %,12.1f:1                                        ║\n", compressedMB > 0 ? decompressedMB / compressedMB : 1));
        sb.append("╠══════════════════════════════════════════════════════════════════════════════╣\n");
        sb.append("║ THROUGHPUT                                                                   ║\n");
        sb.append(String.format("║   Data Transfer Time: %,10d ms    Effective: %,10.2f MB/s          ║\n", dataTransferTimeMs, throughputMBps));
        sb.append(String.format("║   Row Throughput:     %,10.0f rows/sec                                 ║\n", rowsPerSec));
        sb.append("╠══════════════════════════════════════════════════════════════════════════════╣\n");
        sb.append("║ PHASE BREAKDOWN                              Time (ms)    % of Total         ║\n");
        sb.append("╠══════════════════════════════════════════════════════════════════════════════╣\n");
        sb.append(String.format("║   1. Network I/O (socket read)            %,10.1f      %5.1f%%   %s    ║\n", 
            networkMs, networkPct, getBar(networkPct)));
        sb.append(String.format("║   2. Decompression (LZ4/ZLIB/None)        %,10.1f      %5.1f%%   %s    ║\n", 
            decompressMs, decompressPct, getBar(decompressPct)));
        sb.append(String.format("║   3. Direct Binary Parsing (Zero-Arrow)   %,10.1f      %5.1f%%   %s    ║\n", 
            directParseMs, directParsePct, getBar(directParsePct)));
        sb.append(String.format("║   4. Queue Push (producer wait)           %,10.1f      %5.1f%%   %s    ║\n", 
            queuePushMs, queuePushPct, getBar(queuePushPct)));
        sb.append(String.format("║   5. Queue Poll (consumer wait)           %,10.1f      %5.1f%%   %s    ║\n", 
            queuePollMs, queuePollPct, getBar(queuePollPct)));
        sb.append("╠══════════════════════════════════════════════════════════════════════════════╣\n");
        sb.append(String.format("║   Total Profiled:                         %,10.1f ms                   ║\n", totalProfiledMs));
        sb.append(String.format("║   Overhead (unprofiled):                  %,10.1f ms                   ║\n", overheadMs));
        sb.append(String.format("║   Total Wall Time:                        %,10d ms                   ║\n", totalTimeMs));
        sb.append("╠══════════════════════════════════════════════════════════════════════════════╣\n");
        sb.append("║ WAIT EVENTS                                                                  ║\n");
        sb.append(String.format("║   Queue Full Waits:   %,10d    (producer blocked by slow consumer)     ║\n", p.queueFullWaitCount.get()));
        sb.append(String.format("║   Empty Queue Waits:  %,10d    (consumer waiting for network data)     ║\n", p.networkWaitCount.get()));
        sb.append("╠══════════════════════════════════════════════════════════════════════════════╣\n");
        
        // Bottleneck analysis
        String bottleneck = "Unknown";
        String recommendation = "";
        double maxPct = Math.max(networkPct, Math.max(decompressPct, Math.max(directParsePct, 
                        Math.max(queuePollPct, queuePushPct))));
        
        if (networkPct == maxPct && networkPct > 20) {
            bottleneck = "NETWORK I/O";
            recommendation = "Increase batch-size or check network bandwidth";
        } else if (decompressPct == maxPct && decompressPct > 20) {
            bottleneck = "DECOMPRESSION";
            recommendation = "Consider LZ4 compression (requires UDF change) or disable compression";
        } else if (directParsePct == maxPct && directParsePct > 20) {
            bottleneck = "DIRECT PARSING";
            recommendation = "Check for complex expressions or many columns";
        } else if (queuePushPct == maxPct && queuePushPct > 20) {
            bottleneck = "QUEUE FULL (consumer slow)";
            recommendation = "Trino is processing pages slower than the bridge can parse them";
        } else if (p.networkWaitCount.get() > p.batchCount.get()) {
            bottleneck = "NETWORK/TERADATA LATENCY";
            recommendation = "Data arrival is slower than processing; check Teradata side";
        }
        
        sb.append("║ BOTTLENECK ANALYSIS                                                          ║\n");
        sb.append(String.format("║   Primary Bottleneck: %-54s ║\n", bottleneck));
        sb.append(String.format("║   Recommendation:     %-54s ║\n", recommendation));
        sb.append("╚══════════════════════════════════════════════════════════════════════════════╝\n");

        String summary = sb.toString();
        log.info(summary);
        return summary;
    }

    private static String getBar(double pct) {
        int bars = (int) Math.min(10, pct / 5);
        return "█".repeat(bars) + "░".repeat(10 - bars);
    }

    public static void clear(String queryId) {
        profiles.remove(queryId);
    }
}
