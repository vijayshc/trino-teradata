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
package io.trino.stress;

import io.trino.jdbc.TrinoResultSet;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Lightweight stress test runner for the Teradata Direct connector.
 *
 * <p>Goals:
 * <ul>
 *   <li>Identify concurrency bottlenecks</li>
 *   <li>Measure response-time degradation under load</li>
 *   <li>Detect stuck queries / thread starvation</li>
 *   <li>Monitor client-side memory pressure</li>
 * </ul>
 *
 * <p>Usage:
 * <pre>
 *   java -cp target/classes:lib/* io.trino.stress.StressTestRunner [options]
 * </pre>
 *
 * <p>Options:
 * <ul>
 *   <li>{@code --concurrency N} — concurrent workers (default: 10)</li>
 *   <li>{@code --duration N} — test duration seconds (default: 60)</li>
 *   <li>{@code --ramp-up N} — ramp-up seconds (default: 5)</li>
 *   <li>{@code --query-type TYPE} — small|medium|large|mixed (default: mixed)</li>
 *   <li>{@code --url URL} — Trino JDBC URL</li>
 *   <li>{@code --user USER} — Trino user</li>
 * </ul>
 *
 * <p>Defaults for URL/user also come from system properties {@code trino.jdbc.url}
 * / {@code trino.user} or environment variables {@code TRINO_JDBC_URL} / {@code TRINO_USER}.
 */
public class StressTestRunner
{
    private static final String DEFAULT_URL = firstNonBlank(
            System.getProperty("trino.jdbc.url"),
            System.getenv("TRINO_JDBC_URL"),
            "jdbc:trino://localhost:8080/tdexport");
    private static final String DEFAULT_USER = firstNonBlank(
            System.getProperty("trino.user"),
            System.getenv("TRINO_USER"),
            "trino");
    private static final int DEFAULT_CONCURRENCY = 10;
    private static final int DEFAULT_DURATION_SECONDS = 60;
    private static final int DEFAULT_RAMP_UP_SECONDS = 5;

    private final AtomicLong totalQueries = new AtomicLong(0);
    private final AtomicLong successfulQueries = new AtomicLong(0);
    private final AtomicLong failedQueries = new AtomicLong(0);
    private final AtomicLong totalRows = new AtomicLong(0);
    private final AtomicLong totalExecutionTimeNanos = new AtomicLong(0);
    private final AtomicLong mismatchQueries = new AtomicLong(0);
    private final ConcurrentHashMap<String, AtomicLong> errorCounts = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<Long> responseTimes = new ConcurrentLinkedQueue<>();
    private final ConcurrentHashMap<Long, QueryInfo> activeQueries = new ConcurrentHashMap<>();

    // Default workload targets lab fixture tables from testing/setup_*.bteq
    private final List<String> smallQueries = Arrays.asList(
            "select * from trinoexport.test_join_dim");
    private final List<String> mediumQueries = Arrays.asList(
            "select * from trinoexport.test_join_dim");
    private final List<String> largeQueries = Arrays.asList(
            "select * from trinoexport.test_join_dim");
    private final Map<String, Integer> expectedCounts = Map.of(
            "select * from trinoexport.test_join_dim", 5);

    private volatile boolean running = true;
    private String jdbcUrl;
    private String trinoUser;
    private int concurrency;
    private int durationSeconds;
    private int rampUpSeconds;
    private String queryType;

    public static void main(String[] args)
    {
        StressTestRunner runner = new StressTestRunner();
        runner.parseArgs(args);
        runner.run();
    }

    private void parseArgs(String[] args)
    {
        this.jdbcUrl = DEFAULT_URL;
        this.trinoUser = DEFAULT_USER;
        this.concurrency = DEFAULT_CONCURRENCY;
        this.durationSeconds = DEFAULT_DURATION_SECONDS;
        this.rampUpSeconds = DEFAULT_RAMP_UP_SECONDS;
        this.queryType = "mixed";

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--concurrency" -> concurrency = Integer.parseInt(args[++i]);
                case "--duration" -> durationSeconds = Integer.parseInt(args[++i]);
                case "--ramp-up" -> rampUpSeconds = Integer.parseInt(args[++i]);
                case "--query-type" -> queryType = args[++i];
                case "--url" -> jdbcUrl = args[++i];
                case "--user" -> trinoUser = args[++i];
                case "--help" -> {
                    printHelp();
                    System.exit(0);
                }
                default -> {
                    System.err.println("Unknown option: " + args[i]);
                    printHelp();
                    System.exit(2);
                }
            }
        }
    }

    private void printHelp()
    {
        System.out.println("""
            Teradata Direct connector stress test runner

            Usage: java -cp ... io.trino.stress.StressTestRunner [options]

            Options:
              --concurrency N      Concurrent workers (default: 10)
              --duration N         Duration in seconds (default: 60)
              --ramp-up N          Ramp-up in seconds (default: 5)
              --query-type TYPE    small|medium|large|mixed (default: mixed)
              --url URL            Trino JDBC URL
              --user USER          Trino user (default: trino)
              --help               Show this help

            Environment / system properties (used when flags omitted):
              TRINO_JDBC_URL / -Dtrino.jdbc.url
              TRINO_USER     / -Dtrino.user
            """);
    }

    public void run()
    {
        System.out.println("=".repeat(70));
        System.out.println("  TERADATA DIRECT CONNECTOR STRESS TEST");
        System.out.println("=".repeat(70));
        System.out.printf("  Concurrency:    %d threads%n", concurrency);
        System.out.printf("  Duration:       %d seconds%n", durationSeconds);
        System.out.printf("  Ramp-up:        %d seconds%n", rampUpSeconds);
        System.out.printf("  Query Type:     %s%n", queryType);
        System.out.printf("  JDBC URL:       %s%n", jdbcUrl);
        System.out.printf("  User:           %s%n", trinoUser);
        System.out.println("=".repeat(70));

        if (!validateConnectivity()) {
            System.err.println("FATAL: Cannot connect to Trino. Aborting stress test.");
            System.exit(1);
        }

        ExecutorService executor = Executors.newFixedThreadPool(concurrency + 2);
        Instant startTime = Instant.now();

        executor.submit(() -> monitorThread(startTime));
        executor.submit(this::deadlockDetectorThread);

        for (int i = 0; i < concurrency; i++) {
            final int workerId = i;
            executor.submit(() -> workerThread(workerId, startTime));

            if (rampUpSeconds > 0 && i < concurrency - 1) {
                try {
                    Thread.sleep((rampUpSeconds * 1000L) / concurrency);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        try {
            Thread.sleep(durationSeconds * 1000L);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        running = false;

        executor.shutdown();
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                System.out.println("WARNING: Some threads did not terminate gracefully");
                executor.shutdownNow();
            }
        }
        catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        printFinalReport(startTime);
    }

    private boolean validateConnectivity()
    {
        System.out.print("Validating connectivity... ");
        try (Connection conn = DriverManager.getConnection(jdbcUrl, trinoUser, null);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT 1")) {
            if (rs.next()) {
                System.out.println("OK");
                return true;
            }
        }
        catch (Exception e) {
            System.out.println("FAILED: " + e.getMessage());
        }
        return false;
    }

    private void workerThread(int workerId, Instant testStart)
    {
        Random random = new Random(workerId);
        List<String> queries = selectQuerySet();

        while (running) {
            String query = queries.get(random.nextInt(queries.size()));
            long queryId = Thread.currentThread().threadId();

            QueryInfo info = new QueryInfo(query, System.currentTimeMillis(), workerId);
            activeQueries.put(queryId, info);

            try {
                Instant start = Instant.now();
                QueryResult result = executeQuery(query);
                Instant end = Instant.now();

                long durationMs = Duration.between(start, end).toMillis();
                responseTimes.add(durationMs);
                totalExecutionTimeNanos.addAndGet(Duration.between(start, end).toNanos());
                totalRows.addAndGet(result.rows);

                Integer expected = expectedCounts.get(query);
                if (expected != null && result.rows != expected) {
                    mismatchQueries.incrementAndGet();
                    String errorType = String.format("DATA_MISMATCH (got %d, expected %d)", result.rows, expected);
                    errorCounts.computeIfAbsent(errorType, k -> new AtomicLong(0)).incrementAndGet();
                    System.out.printf("[%s] MISMATCH: QueryId: %s, Worker: %d, Rows: %d, Expected: %d, Query: %s%n",
                            LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss.SSS")),
                            result.queryId, workerId, result.rows, expected, query);
                }
                else {
                    successfulQueries.incrementAndGet();
                }
            }
            catch (Exception e) {
                failedQueries.incrementAndGet();
                String errorType = categorizeError(e);
                errorCounts.computeIfAbsent(errorType, k -> new AtomicLong(0)).incrementAndGet();
            }
            finally {
                totalQueries.incrementAndGet();
                activeQueries.remove(queryId);
            }

            try {
                Thread.sleep(10);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private List<String> selectQuerySet()
    {
        return switch (queryType.toLowerCase()) {
            case "small" -> smallQueries;
            case "medium" -> mediumQueries;
            case "large" -> largeQueries;
            default -> {
                List<String> all = new ArrayList<>();
                all.addAll(smallQueries);
                all.addAll(mediumQueries);
                all.addAll(largeQueries);
                yield all;
            }
        };
    }

    private QueryResult executeQuery(String query)
            throws SQLException
    {
        int rows = 0;
        String trinoQueryId = "unknown";
        try (Connection conn = DriverManager.getConnection(jdbcUrl, trinoUser, null);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(query)) {
            try {
                if (rs.isWrapperFor(TrinoResultSet.class)) {
                    trinoQueryId = rs.unwrap(TrinoResultSet.class).getQueryId();
                }
            }
            catch (Exception ignored) {
            }

            while (rs.next()) {
                rows++;
            }
        }
        return new QueryResult(rows, trinoQueryId);
    }

    private String categorizeError(Exception e)
    {
        String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();

        if (msg.contains("timeout") || msg.contains("Timeout")) {
            return "TIMEOUT";
        }
        if (msg.contains("connection") || msg.contains("Connection")) {
            return "CONNECTION";
        }
        if (msg.contains("memory") || msg.contains("Memory") || msg.contains("OOM")) {
            return "MEMORY";
        }
        if (msg.contains("thread") || msg.contains("Thread")) {
            return "THREAD_POOL";
        }
        if (msg.contains("queue") || msg.contains("Queue")) {
            return "QUEUE_FULL";
        }
        if (msg.contains("socket") || msg.contains("Socket")) {
            return "SOCKET";
        }
        if (msg.contains("rejected") || msg.contains("Rejected")) {
            return "REJECTED";
        }

        return "OTHER: " + msg.substring(0, Math.min(50, msg.length()));
    }

    private void monitorThread(Instant testStart)
    {
        Runtime runtime = Runtime.getRuntime();

        System.out.println("\n--- LIVE METRICS ---");
        System.out.printf("%-8s %8s %8s %8s %8s %10s %8s%n",
                "Time", "Total", "Success", "Failed", "Mismatch", "Avg(ms)", "Memory");

        while (running) {
            try {
                Thread.sleep(5000);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }

            long elapsed = Duration.between(testStart, Instant.now()).getSeconds();
            long total = totalQueries.get();
            long success = successfulQueries.get();
            long failed = failedQueries.get();
            long mismatches = mismatchQueries.get();
            long avgMs = total > 0 ? totalExecutionTimeNanos.get() / total / 1_000_000 : 0;
            long usedMb = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);

            System.out.printf("%-8s %8d %8d %8d %8d %10d %7dMB%n",
                    formatTime(elapsed), total, success, failed, mismatches, avgMs, usedMb);
        }
    }

    private void deadlockDetectorThread()
    {
        while (running) {
            try {
                Thread.sleep(10000);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }

            long now = System.currentTimeMillis();
            List<QueryInfo> stuckQueries = new ArrayList<>();

            for (QueryInfo info : activeQueries.values()) {
                if (now - info.startTime > 30000) {
                    stuckQueries.add(info);
                }
            }

            if (!stuckQueries.isEmpty()) {
                System.out.println("\n!!! POTENTIAL BOTTLENECK DETECTED !!!");
                System.out.printf("  %d queries stuck for >30 seconds:%n", stuckQueries.size());
                for (QueryInfo info : stuckQueries) {
                    System.out.printf("    Worker %d: %s (running %d ms)%n",
                            info.workerId,
                            info.query.substring(0, Math.min(40, info.query.length())),
                            now - info.startTime);
                }
            }
        }
    }

    private void printFinalReport(Instant testStart)
    {
        System.out.println("\n" + "=".repeat(70));
        System.out.println("  STRESS TEST FINAL REPORT");
        System.out.println("=".repeat(70));

        long durationSec = Duration.between(testStart, Instant.now()).getSeconds();
        long total = totalQueries.get();
        long success = successfulQueries.get();
        long failed = failedQueries.get();
        long rows = totalRows.get();

        System.out.printf("  Duration:           %s%n", formatTime(durationSec));
        System.out.printf("  Total Queries:      %,d%n", total);
        System.out.printf("  Successful:         %,d (%.1f%%)%n", success, total > 0 ? 100.0 * success / total : 0);
        System.out.printf("  Failed:             %,d (%.1f%%)%n", failed, total > 0 ? 100.0 * failed / total : 0);
        System.out.printf("  Data Mismatches:    %,d (%.1f%%)%n",
                mismatchQueries.get(), total > 0 ? 100.0 * mismatchQueries.get() / total : 0);
        System.out.printf("  Total Rows:         %,d%n", rows);
        System.out.printf("  Throughput:         %.2f queries/sec%n", durationSec > 0 ? (double) total / durationSec : 0);

        List<Long> times = new ArrayList<>(responseTimes);
        if (!times.isEmpty()) {
            Collections.sort(times);
            System.out.println("\n  Response Time Percentiles:");
            System.out.printf("    P50:  %,d ms%n", percentile(times, 50));
            System.out.printf("    P90:  %,d ms%n", percentile(times, 90));
            System.out.printf("    P95:  %,d ms%n", percentile(times, 95));
            System.out.printf("    P99:  %,d ms%n", percentile(times, 99));
            System.out.printf("    Max:  %,d ms%n", times.get(times.size() - 1));
        }

        if (!errorCounts.isEmpty()) {
            System.out.println("\n  Error Breakdown:");
            errorCounts.entrySet().stream()
                    .sorted((a, b) -> Long.compare(b.getValue().get(), a.getValue().get()))
                    .forEach(e -> System.out.printf("    %-20s: %,d%n", e.getKey(), e.getValue().get()));
        }

        System.out.println("\n  BOTTLENECK ANALYSIS:");
        analyzeBottlenecks(times);

        System.out.println("=".repeat(70));
    }

    private void analyzeBottlenecks(List<Long> times)
    {
        if (times.isEmpty()) {
            System.out.println("    No data to analyze.");
            return;
        }

        long p50 = percentile(times, 50);
        long p99 = percentile(times, 99);
        double failRate = totalQueries.get() > 0 ? 100.0 * failedQueries.get() / totalQueries.get() : 0;

        if (p99 > p50 * 5) {
            System.out.println("    HIGH TAIL LATENCY: P99 is 5x+ P50");
            System.out.println("       -> Indicates thread pool saturation or lock contention");
            System.out.println("       -> Consider: increase max-bridge-threads, max-query-concurrency");
        }

        if (failRate > 5) {
            System.out.println("    HIGH FAILURE RATE: " + String.format("%.1f%%", failRate));
            if (errorCounts.containsKey("TIMEOUT")) {
                System.out.println("       -> Timeouts — increase query timeout or reduce concurrency");
            }
            if (errorCounts.containsKey("THREAD_POOL") || errorCounts.containsKey("REJECTED")) {
                System.out.println("       -> Thread pool exhaustion — increase max-bridge-threads");
            }
            if (errorCounts.containsKey("QUEUE_FULL")) {
                System.out.println("       -> Queue full — increase bridge-queue-capacity");
            }
            if (errorCounts.containsKey("MEMORY")) {
                System.out.println("       -> Memory pressure — increase JVM heap or reduce buffer sizes");
            }
        }

        long throughput = durationSeconds > 0 ? totalQueries.get() / durationSeconds : 0;
        if (throughput < concurrency / 2.0) {
            System.out.println("    LOW THROUGHPUT: " + throughput + " qps (expected ~" + concurrency + ")");
            System.out.println("       -> System is bottlenecked; check Trino server.log");
        }

        if (p99 < 1000 && failRate < 1 && throughput >= concurrency / 2.0) {
            System.out.println("    HEALTHY: System handling load well");
        }
    }

    private long percentile(List<Long> sorted, int p)
    {
        if (sorted.isEmpty()) {
            return 0;
        }
        int index = (int) Math.ceil(p / 100.0 * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(index, sorted.size() - 1)));
    }

    private String formatTime(long seconds)
    {
        return String.format("%02d:%02d", seconds / 60, seconds % 60);
    }

    private static String firstNonBlank(String... values)
    {
        for (String v : values) {
            if (v != null && !v.isBlank()) {
                return v;
            }
        }
        return null;
    }

    private static class QueryResult
    {
        final int rows;
        final String queryId;

        QueryResult(int rows, String queryId)
        {
            this.rows = rows;
            this.queryId = queryId;
        }
    }

    private static class QueryInfo
    {
        final String query;
        final long startTime;
        final int workerId;

        QueryInfo(String query, long startTime, int workerId)
        {
            this.query = query;
            this.startTime = startTime;
            this.workerId = workerId;
        }
    }
}
