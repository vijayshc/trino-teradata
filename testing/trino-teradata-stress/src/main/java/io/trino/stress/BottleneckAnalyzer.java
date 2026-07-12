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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Component-level bottleneck analyzer for the Teradata Direct connector.
 *
 * <p>Targeted tests:
 * <ol>
 *   <li>Connection pool — JDBC connection overhead</li>
 *   <li>Thread pool — concurrent query handling</li>
 *   <li>Buffer queue — data volume / backpressure</li>
 *   <li>Serialization — parse/decompression overhead</li>
 *   <li>Load escalation — find breaking concurrency</li>
 * </ol>
 *
 * <p>Usage:
 * <pre>
 *   java -cp ... io.trino.stress.BottleneckAnalyzer [test-name] [--url URL] [--user USER]
 * </pre>
 */
public class BottleneckAnalyzer
{
    private final String jdbcUrl;
    private final String trinoUser;

    public BottleneckAnalyzer(String jdbcUrl, String trinoUser)
    {
        this.jdbcUrl = jdbcUrl;
        this.trinoUser = trinoUser;
    }

    public static void main(String[] args)
    {
        String testName = "all";
        String jdbcUrl = firstNonBlank(
                System.getProperty("trino.jdbc.url"),
                System.getenv("TRINO_JDBC_URL"),
                "jdbc:trino://localhost:8080/tdexport");
        String trinoUser = firstNonBlank(
                System.getProperty("trino.user"),
                System.getenv("TRINO_USER"),
                "trino");

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--url" -> jdbcUrl = args[++i];
                case "--user" -> trinoUser = args[++i];
                case "--help" -> {
                    printHelp();
                    System.exit(0);
                }
                default -> {
                    if (args[i].startsWith("-")) {
                        System.err.println("Unknown option: " + args[i]);
                        printHelp();
                        System.exit(2);
                    }
                    testName = args[i];
                }
            }
        }

        BottleneckAnalyzer analyzer = new BottleneckAnalyzer(jdbcUrl, trinoUser);

        System.out.println("=".repeat(70));
        System.out.println("  BOTTLENECK ANALYZER - Component Level Stress Testing");
        System.out.println("=".repeat(70));
        System.out.printf("  JDBC URL: %s%n", jdbcUrl);
        System.out.printf("  User:     %s%n", trinoUser);

        switch (testName.toLowerCase()) {
            case "connection" -> analyzer.testConnectionPool();
            case "threadpool" -> analyzer.testThreadPoolSaturation();
            case "buffer" -> analyzer.testBufferQueuePressure();
            case "serialize" -> analyzer.testSerializationOverhead();
            case "escalation" -> analyzer.testLoadEscalation();
            default -> {
                analyzer.testConnectionPool();
                analyzer.testThreadPoolSaturation();
                analyzer.testBufferQueuePressure();
                analyzer.testSerializationOverhead();
                analyzer.testLoadEscalation();
            }
        }

        System.out.println("\n" + "=".repeat(70));
        System.out.println("  ANALYSIS COMPLETE");
        System.out.println("=".repeat(70));
    }

    private static void printHelp()
    {
        System.out.println("""
            Bottleneck analyzer

            Usage: java -cp ... io.trino.stress.BottleneckAnalyzer [test] [options]

            Tests:
              connection | threadpool | buffer | serialize | escalation | all

            Options:
              --url URL
              --user USER
            """);
    }

    private void testConnectionPool()
    {
        System.out.println("\n--- TEST 1: CONNECTION POOL LIMITS ---");

        int[] connectionCounts = {5, 10, 20, 50, 100};

        for (int count : connectionCounts) {
            List<Connection> connections = new ArrayList<>();
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger failCount = new AtomicInteger(0);

            Instant start = Instant.now();
            ExecutorService executor = Executors.newFixedThreadPool(count);
            CountDownLatch latch = new CountDownLatch(count);

            for (int i = 0; i < count; i++) {
                executor.submit(() -> {
                    try {
                        Connection conn = DriverManager.getConnection(jdbcUrl, trinoUser, null);
                        synchronized (connections) {
                            connections.add(conn);
                        }
                        successCount.incrementAndGet();
                    }
                    catch (Exception e) {
                        failCount.incrementAndGet();
                    }
                    finally {
                        latch.countDown();
                    }
                });
            }

            try {
                boolean completed = latch.await(30, TimeUnit.SECONDS);
                long durationMs = Duration.between(start, Instant.now()).toMillis();

                System.out.printf("  %3d connections: %d succeeded, %d failed, %d ms%s%n",
                        count, successCount.get(), failCount.get(), durationMs,
                        completed ? "" : " (TIMEOUT)");

                if (failCount.get() > 0) {
                    System.out.printf("      Connection failures detected at %d connections%n", count);
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            for (Connection conn : connections) {
                try {
                    conn.close();
                }
                catch (Exception ignored) {
                }
            }
            executor.shutdownNow();
        }
    }

    private void testThreadPoolSaturation()
    {
        System.out.println("\n--- TEST 2: THREAD POOL SATURATION ---");

        int[] concurrencyLevels = {5, 10, 25, 50, 100};
        String query = "SELECT * FROM dbc.databases LIMIT 1";

        for (int concurrency : concurrencyLevels) {
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger failCount = new AtomicInteger(0);
            ConcurrentLinkedQueue<Long> responseTimes = new ConcurrentLinkedQueue<>();

            ExecutorService executor = Executors.newFixedThreadPool(concurrency);
            int queriesPerThread = 10;
            CountDownLatch latch = new CountDownLatch(concurrency * queriesPerThread);

            Instant start = Instant.now();

            for (int i = 0; i < concurrency; i++) {
                executor.submit(() -> {
                    for (int j = 0; j < queriesPerThread; j++) {
                        try {
                            Instant qStart = Instant.now();
                            executeQuery(query);
                            responseTimes.add(Duration.between(qStart, Instant.now()).toMillis());
                            successCount.incrementAndGet();
                        }
                        catch (Exception e) {
                            failCount.incrementAndGet();
                        }
                        finally {
                            latch.countDown();
                        }
                    }
                });
            }

            try {
                boolean completed = latch.await(120, TimeUnit.SECONDS);
                long wallClockMs = Duration.between(start, Instant.now()).toMillis();

                List<Long> times = new ArrayList<>(responseTimes);
                Collections.sort(times);
                long p50 = times.isEmpty() ? 0 : times.get(times.size() / 2);
                long p99 = times.isEmpty() ? 0 : times.get(Math.min(times.size() - 1, (int) (times.size() * 0.99)));

                System.out.printf("  %3d threads: %d ok, %d fail | P50: %d ms, P99: %d ms | Total: %d ms%s%n",
                        concurrency, successCount.get(), failCount.get(),
                        p50, p99, wallClockMs,
                        completed ? "" : " (TIMEOUT)");

                if (p99 > p50 * 3 && p50 > 0) {
                    System.out.printf("      High tail latency at %d threads (thread contention)%n", concurrency);
                }
                if (failCount.get() > 0) {
                    System.out.printf("      Failures at %d threads (pool exhaustion)%n", concurrency);
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            executor.shutdownNow();
        }
    }

    private void testBufferQueuePressure()
    {
        System.out.println("\n--- TEST 3: BUFFER QUEUE PRESSURE (Data Volume) ---");

        Map<String, String> testQueries = new LinkedHashMap<>();
        testQueries.put("1 row", "SELECT * FROM dbc.databases LIMIT 1");
        testQueries.put("10 rows", "SELECT * FROM dbc.tables LIMIT 10");
        testQueries.put("100 rows", "SELECT * FROM dbc.tables LIMIT 100");
        testQueries.put("1000 rows", "SELECT * FROM dbc.columns LIMIT 1000");

        int concurrency = 20;
        int queriesEach = 5;

        for (Map.Entry<String, String> entry : testQueries.entrySet()) {
            String label = entry.getKey();
            String query = entry.getValue();

            AtomicLong totalRows = new AtomicLong(0);
            AtomicInteger failCount = new AtomicInteger(0);

            ExecutorService executor = Executors.newFixedThreadPool(concurrency);
            CountDownLatch latch = new CountDownLatch(concurrency * queriesEach);

            Instant start = Instant.now();

            for (int i = 0; i < concurrency; i++) {
                executor.submit(() -> {
                    for (int j = 0; j < queriesEach; j++) {
                        try {
                            totalRows.addAndGet(executeQueryAndCount(query));
                        }
                        catch (Exception e) {
                            failCount.incrementAndGet();
                        }
                        finally {
                            latch.countDown();
                        }
                    }
                });
            }

            try {
                boolean completed = latch.await(120, TimeUnit.SECONDS);
                long wallClockMs = Duration.between(start, Instant.now()).toMillis();
                long rows = totalRows.get();
                double rowsPerSec = wallClockMs > 0 ? (rows * 1000.0 / wallClockMs) : 0;

                System.out.printf("  %-12s: %,8d rows | %.1f rows/sec | %d failures%s%n",
                        label, rows, rowsPerSec, failCount.get(),
                        completed ? "" : " (TIMEOUT)");
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            executor.shutdownNow();
        }
    }

    private void testSerializationOverhead()
    {
        System.out.println("\n--- TEST 4: SERIALIZATION/DECOMPRESSION OVERHEAD ---");

        Map<String, String> typeQueries = new LinkedHashMap<>();
        typeQueries.put("Integers", "SELECT * FROM dbc.tables WHERE tablekind = 'T' LIMIT 20");
        typeQueries.put("Mixed", "SELECT databasename, tablename, tablekind, commentstring FROM dbc.tables LIMIT 20");
        typeQueries.put("Strings", "SELECT databasename, tablename, commentstring FROM dbc.tables LIMIT 20");
        typeQueries.put("Large", "SELECT * FROM dbc.columns LIMIT 50");

        int iterations = 10;

        for (Map.Entry<String, String> entry : typeQueries.entrySet()) {
            String label = entry.getKey();
            String query = entry.getValue();

            List<Long> times = new ArrayList<>();
            int totalRows = 0;
            int failures = 0;

            for (int i = 0; i < iterations; i++) {
                try {
                    Instant start = Instant.now();
                    int rows = executeQueryAndCount(query);
                    times.add(Duration.between(start, Instant.now()).toMillis());
                    totalRows += rows;
                }
                catch (Exception e) {
                    failures++;
                }
            }

            if (!times.isEmpty()) {
                Collections.sort(times);
                long median = times.get(times.size() / 2);
                long min = times.get(0);
                long max = times.get(times.size() - 1);

                System.out.printf("  %-12s: Median %d ms, Min %d ms, Max %d ms (rows: %d, failures: %d)%n",
                        label, median, min, max, totalRows / iterations, failures);

                if (max > median * 2) {
                    System.out.printf("      High variance for %s (possible GC or contention)%n", label);
                }
            }
        }
    }

    private void testLoadEscalation()
    {
        System.out.println("\n--- TEST 5: LOAD ESCALATION (Finding Breaking Point) ---");

        String query = "SELECT * FROM dbc.tables LIMIT 10";

        System.out.println("  Gradually increasing load until failure or timeout...");
        System.out.printf("  %-12s %-12s %-12s %-12s %-15s%n",
                "Concurrency", "Success", "Failed", "Avg(ms)", "Status");

        for (int concurrency = 10; concurrency <= 200; concurrency += 20) {
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger failCount = new AtomicInteger(0);
            AtomicLong totalTime = new AtomicLong(0);

            int queriesTotal = concurrency * 3;
            ExecutorService executor = Executors.newFixedThreadPool(concurrency);
            CountDownLatch latch = new CountDownLatch(queriesTotal);

            for (int i = 0; i < queriesTotal; i++) {
                executor.submit(() -> {
                    try {
                        Instant qStart = Instant.now();
                        executeQuery(query);
                        totalTime.addAndGet(Duration.between(qStart, Instant.now()).toMillis());
                        successCount.incrementAndGet();
                    }
                    catch (Exception e) {
                        failCount.incrementAndGet();
                    }
                    finally {
                        latch.countDown();
                    }
                });
            }

            String status;
            try {
                boolean completed = latch.await(60, TimeUnit.SECONDS);
                long avgMs = successCount.get() > 0 ? totalTime.get() / successCount.get() : 0;

                if (!completed) {
                    status = "TIMEOUT";
                }
                else if (failCount.get() > queriesTotal * 0.1) {
                    status = "HIGH FAILURES";
                }
                else if (avgMs > 5000) {
                    status = "SLOW";
                }
                else {
                    status = "OK";
                }

                System.out.printf("  %-12d %-12d %-12d %-12d %-15s%n",
                        concurrency, successCount.get(), failCount.get(), avgMs, status);

                if (!status.equals("OK")) {
                    System.out.println("\n  Breaking point identified at concurrency: " + concurrency);
                    executor.shutdownNow();
                    break;
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                executor.shutdownNow();
                break;
            }

            executor.shutdownNow();
        }
    }

    private void executeQuery(String query)
            throws SQLException
    {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, trinoUser, null);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(query)) {
            while (rs.next()) {
                // consume
            }
        }
    }

    private int executeQueryAndCount(String query)
            throws SQLException
    {
        int count = 0;
        try (Connection conn = DriverManager.getConnection(jdbcUrl, trinoUser, null);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(query)) {
            while (rs.next()) {
                count++;
            }
        }
        return count;
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
}
