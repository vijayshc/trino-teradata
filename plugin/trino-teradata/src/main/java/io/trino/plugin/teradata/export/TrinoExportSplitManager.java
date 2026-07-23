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
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.*;
import io.trino.spi.NodeManager;
import io.trino.spi.Node;

import com.google.inject.Inject;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.PreDestroy;

/**
 * Split manager for the Teradata Export Connector.
 *
 * <p>AMP routing uses {@code worker-advertised-addresses} (host:bridgePort list).
 * Splits must be scheduled onto the Trino JVMs that own those bridges. When multiple
 * Trino processes share a host (common lab layout), we use each node's HTTP
 * {@link HostAddress} so the scheduler can distinguish them.
 */
public class TrinoExportSplitManager implements ConnectorSplitManager {
    private static final Logger log = Logger.get(TrinoExportSplitManager.class);

    private static final long HEALTH_CACHE_TTL_MS = 30_000L;

    private final NodeManager nodeManager;
    private final TrinoExportConfig config;
    private final ExecutorService executor;
    private final TeradataClient teradataClient;
    private final TeradataQueryBuilder queryBuilder;
    private final TeradataConnectionPool connectionPool;
    private final Semaphore queryConcurrency;
    private final Map<String, CachedHealth> healthCache = new ConcurrentHashMap<>();

    private record CachedHealth(boolean healthy, long checkedAtMs) {}

    /** One data-plane endpoint: AMP target + Trino schedule address. */
    private record BridgeEndpoint(String advertisedHostPort, HostAddress scheduleAddress) {}

    @Inject
    public TrinoExportSplitManager(
            NodeManager nodeManager,
            TrinoExportConfig config,
            TeradataClient teradataClient,
            TeradataQueryBuilder queryBuilder,
            TeradataConnectionPool connectionPool) {
        this.nodeManager = nodeManager;
        this.config = config;
        this.teradataClient = teradataClient;
        this.queryBuilder = queryBuilder;
        this.connectionPool = connectionPool;
        this.queryConcurrency = new Semaphore(Math.max(1, config.getMaxQueryConcurrency()), true);

        int corePoolSize = config.getExecutorCorePoolSize();
        int maxThreads = Math.max(config.getMaxQueryConcurrency() * 2, corePoolSize);
        this.executor = new ThreadPoolExecutor(
                corePoolSize,
                maxThreads,
                30L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(Math.max(100, config.getMaxQueryConcurrency() * 4)),
                r -> {
                    Thread t = new Thread(r, "teradata-split-executor");
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.CallerRunsPolicy());

        log.info("TrinoExportSplitManager initialized: corePoolSize=%d, maxThreads=%d, maxQueryConcurrency=%d",
                corePoolSize, maxThreads, config.getMaxQueryConcurrency());
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdownNow();
        log.info("TrinoExportSplitManager executor shutdown");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint) {

        List<BridgeEndpoint> endpoints = resolveBridgeEndpoints();
        if (endpoints.isEmpty()) {
            Node fallback = nodeManager.getCurrentNode();
            String hp = resolveToIp(fallback.getHost()) + ":" + config.getBridgePort();
            endpoints = List.of(new BridgeEndpoint(hp, scheduleAddress(fallback)));
        }

        JdbcTableHandle tableHandle = (JdbcTableHandle) table;
        String baseQueryId = session.getQueryId();
        log.debug("Generating splits for query %s, user: %s, endpoints: %d",
                baseQueryId, session.getUser(), endpoints.size());

        String tableName = tableHandle.isNamedRelation()
                ? tableHandle.getRequiredNamedRelation().getSchemaTableName().toString()
                : "query";

        String tableHash = Integer.toHexString(tableName.hashCode() & 0x7FFFFFFF);
        String randomSuffix = Long.toHexString(System.nanoTime() & 0xFFFFF);
        String splitId = baseQueryId + "_" + tableHash + "_" + randomSuffix;

        String dynamicToken = java.util.UUID.randomUUID().toString();
        DataBufferRegistry.registerDynamicToken(splitId, dynamicToken);

        String allWorkerIps = endpoints.stream()
                .map(BridgeEndpoint::advertisedHostPort)
                .reduce((a, b) -> a + "," + b)
                .orElse(config.getTrinoAddress() + ":" + config.getBridgePort());

        log.debug("Registering split %s for table %s. Worker IPs: %s", splitId, tableName, allWorkerIps);

        List<ConnectorSplit> splits = new ArrayList<>();
        int splitsPerWorker = config.getSplitsPerWorker();
        for (BridgeEndpoint endpoint : endpoints) {
            // Use host:httpPort form so co-located JVMs are distinguishable to the scheduler
            String scheduleHost = endpoint.scheduleAddress().getHostText();
            if (endpoint.scheduleAddress().hasPort()) {
                scheduleHost = endpoint.scheduleAddress().getHostText() + ":" + endpoint.scheduleAddress().getPort();
            }
            for (int i = 0; i < splitsPerWorker; i++) {
                splits.add(new TrinoExportSplit(scheduleHost, splitId, allWorkerIps, dynamicToken));
            }
        }

        log.debug("Created %d splits for query %s (%d per endpoint, %d endpoints)",
                splits.size(), splitId, splitsPerWorker, endpoints.size());

        return new TrinoExportDynamicFilteringSplitSource(
                splits, dynamicFilter, tableHandle, splitId, allWorkerIps, dynamicToken,
                session.getUser(), config, executor, teradataClient, queryBuilder, session, connectionPool,
                queryConcurrency);
    }

    /**
     * Build ordered data-plane endpoints. Prefer configured advertised addresses (AMP-visible),
     * map each to a Trino node for task scheduling, and health-check the actual bridge port.
     */
    private List<BridgeEndpoint> resolveBridgeEndpoints() {
        List<Node> allNodes = new ArrayList<>();
        allNodes.addAll(nodeManager.getWorkerNodes());
        // Coordinator may own a bridge when co-located multi-instance uses distinct ports
        Node current = nodeManager.getCurrentNode();
        if (allNodes.stream().noneMatch(n -> n.getNodeIdentifier().equals(current.getNodeIdentifier()))) {
            allNodes.add(current);
        }
        // Stable order for deterministic AMP index mapping
        allNodes.sort(Comparator.comparing(Node::getNodeIdentifier));

        String advertised = config.getWorkerAdvertisedAddresses();
        List<String> advertisedList = parseCsv(advertised);

        List<BridgeEndpoint> endpoints = new ArrayList<>();
        if (!advertisedList.isEmpty()) {
            for (int i = 0; i < advertisedList.size(); i++) {
                String hostPort = advertisedList.get(i);
                if (config.isWorkerHealthCheckEnabled() && !isBridgeHealthy(hostPort)) {
                    log.warn("Advertised bridge %s is unhealthy; skipping", hostPort);
                    continue;
                }
                // Index-aligned mapping: advertised[i] ↔ sorted Trino nodes[i].
                // Critical for co-located multi-JVM labs where many nodes share one IP
                // but own distinct bridge ports (e.g. :9998 vs :9999).
                Node node = allNodes.isEmpty() ? current : allNodes.get(Math.min(i, allNodes.size() - 1));
                String advHost = hostPort.split(":")[0];
                List<Node> hostMatches = new ArrayList<>();
                for (Node n : allNodes) {
                    if (hostsMatch(n.getHost(), advHost)) {
                        hostMatches.add(n);
                    }
                }
                if (hostMatches.size() == 1) {
                    node = hostMatches.get(0);
                }
                else if (hostMatches.size() > 1) {
                    // Multiple JVMs on same host: keep index alignment among matches
                    node = hostMatches.get(Math.min(i, hostMatches.size() - 1));
                }
                endpoints.add(new BridgeEndpoint(hostPort, scheduleAddress(node)));
            }
            if (!endpoints.isEmpty()) {
                return endpoints;
            }
        }

        // Fallback: derive from Trino nodes + local bridge port
        for (Node node : allNodes) {
            String hostPort = resolveToIp(node.getHost()) + ":" + config.getBridgePort();
            if (config.isWorkerHealthCheckEnabled() && !isBridgeHealthy(hostPort)) {
                continue;
            }
            endpoints.add(new BridgeEndpoint(hostPort, scheduleAddress(node)));
        }
        return endpoints;
    }

    private static HostAddress scheduleAddress(Node node) {
        try {
            // Prefer host:httpPort so co-located processes are distinct
            return node.getHostAndPort();
        }
        catch (RuntimeException e) {
            return HostAddress.fromString(node.getHost());
        }
    }

    private static boolean hostsMatch(String a, String b) {
        if (a == null || b == null) {
            return false;
        }
        if (a.equalsIgnoreCase(b)) {
            return true;
        }
        try {
            return InetAddress.getByName(a).getHostAddress().equals(InetAddress.getByName(b).getHostAddress());
        }
        catch (Exception e) {
            return false;
        }
    }

    private static List<String> parseCsv(String csv) {
        List<String> out = new ArrayList<>();
        if (csv == null || csv.isBlank()) {
            return out;
        }
        for (String part : csv.split(",")) {
            String t = part.trim();
            if (!t.isEmpty()) {
                out.add(t);
            }
        }
        return out;
    }

    private String resolveToIp(String host) {
        try {
            return InetAddress.getByName(host).getHostAddress();
        }
        catch (Exception e) {
            return host;
        }
    }

    private boolean isBridgeHealthy(String hostPort) {
        long now = System.currentTimeMillis();
        CachedHealth cached = healthCache.get(hostPort);
        if (cached != null && (now - cached.checkedAtMs()) < HEALTH_CACHE_TTL_MS) {
            return cached.healthy();
        }
        boolean healthy = probeHostPort(hostPort);
        healthCache.put(hostPort, new CachedHealth(healthy, now));
        return healthy;
    }

    private boolean probeHostPort(String hostPort) {
        try {
            String[] parts = hostPort.split(":");
            String host = parts[0];
            int port = parts.length > 1 ? Integer.parseInt(parts[1]) : config.getBridgePort();
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(host, port), config.getWorkerHealthCheckTimeoutMs());
                return true;
            }
        }
        catch (Exception e) {
            log.warn("Bridge %s is unhealthy: %s", hostPort, e.getMessage());
            return false;
        }
    }
}
