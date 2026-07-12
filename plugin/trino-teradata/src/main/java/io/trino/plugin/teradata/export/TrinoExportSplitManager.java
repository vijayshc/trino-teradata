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
import io.trino.spi.connector.*;
import io.trino.spi.NodeManager;
import io.trino.spi.Node;

import com.google.inject.Inject;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

import javax.annotation.PreDestroy;

/**
 * Split manager for the Teradata Export Connector.
 * 
 * Creates splits for each Trino worker node and returns a TrinoExportDynamicFilteringSplitSource
 * that uses Trino's JDBC infrastructure (JdbcTableHandle) for SQL generation.
 */
public class TrinoExportSplitManager implements ConnectorSplitManager {
    private static final Logger log = Logger.get(TrinoExportSplitManager.class);
    
    private final NodeManager nodeManager;
    private final TrinoExportConfig config;
    private final ExecutorService executor;
    private final TeradataClient teradataClient;
    private final TeradataQueryBuilder queryBuilder;
    private final TeradataConnectionPool connectionPool;

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
        
        int corePoolSize = config.getExecutorCorePoolSize();
        int maxThreads = config.getMaxQueryConcurrency();
        this.executor = new ThreadPoolExecutor(
                corePoolSize,
                maxThreads,
                30L, java.util.concurrent.TimeUnit.SECONDS,
                new SynchronousQueue<>(),  // Immediate thread spawning, no queueing
                r -> {
                    Thread t = new Thread(r, "teradata-split-executor");
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
        
        log.info("TrinoExportSplitManager initialized: corePoolSize=%d, maxThreads=%d, connectionPool enabled", corePoolSize, maxThreads);
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
        
        List<Node> workers = new ArrayList<>(nodeManager.getWorkerNodes());
        if (config.isWorkerHealthCheckEnabled()) {
            workers = workers.stream()
                    .filter(this::isWorkerHealthy)
                    .collect(Collectors.toList());
            log.info("Workers after health check: %d", workers.size());
        }
        
        if (workers.isEmpty()) {
            workers.add(nodeManager.getCurrentNode());
        }
        
        // Now using JdbcTableHandle from Trino's JDBC infrastructure
        JdbcTableHandle tableHandle = (JdbcTableHandle) table;
        String baseQueryId = session.getQueryId();
        String trinoUser = session.getUser();
        log.info("Generating splits for query %s, user: %s, workers: %d", baseQueryId, trinoUser, workers.size());
        
        String tableName = tableHandle.isNamedRelation() 
                ? tableHandle.getRequiredNamedRelation().getSchemaTableName().toString()
                : "query";
        
        String tableHash = Integer.toHexString(tableName.hashCode() & 0x7FFFFFFF);
        String randomSuffix = Long.toHexString(System.nanoTime() & 0xFFFFF);
        String splitId = baseQueryId + "_" + tableHash + "_" + randomSuffix;
        
        String dynamicToken = java.util.UUID.randomUUID().toString();
        log.info("Generated dynamic token for query %s", splitId);
        
        DataBufferRegistry.registerDynamicToken(splitId, dynamicToken);

        String allWorkerIps = buildWorkerIpList(workers);

        log.info("Registering split %s for table %s (query %s). Worker IPs: %s", splitId, tableName, baseQueryId, allWorkerIps);

        List<ConnectorSplit> splits = new ArrayList<>();
        int splitsPerWorker = config.getSplitsPerWorker();
        for (Node node : workers) {
            for (int i = 0; i < splitsPerWorker; i++) {
                splits.add(new TrinoExportSplit(node.getHost(), splitId, allWorkerIps, dynamicToken));
            }
        }

        // NOTE: Do NOT register query buffer here - each worker's PageSource will register
        // with its own per-worker consumer count. Registering here with total count causes
        // wrong expectedConsumers on coordinator when it also runs tasks.
        log.info("Created %d splits for query %s (%d per worker, %d workers)", 
                splits.size(), splitId, splitsPerWorker, workers.size());

        return new TrinoExportDynamicFilteringSplitSource(
                splits, dynamicFilter, tableHandle, splitId, allWorkerIps, dynamicToken, 
                session.getUser(), config, executor, teradataClient, queryBuilder, session, connectionPool);
    }
    
    private String buildWorkerIpList(List<Node> workers) {
        String advertisedAddresses = config.getWorkerAdvertisedAddresses();
        if (advertisedAddresses != null && !advertisedAddresses.isEmpty()) {
            log.info("Using configured advertised addresses: %s", advertisedAddresses);
            return advertisedAddresses;
        }
        
        if (workers.size() > 1) {
            return workers.stream()
                    .map(node -> resolveToIp(node.getHost()) + ":" + config.getBridgePort())
                    .distinct()
                    .collect(Collectors.joining(","));
        } else {
            return config.getTrinoAddress() + ":" + config.getBridgePort();
        }
    }
    
    private String resolveToIp(String host) {
        try {
            InetAddress addr = InetAddress.getByName(host);
            String ip = addr.getHostAddress();
            log.debug("Resolved %s -> %s", host, ip);
            return ip;
        } catch (Exception e) {
            log.warn("Failed to resolve hostname %s, using as-is: %s", host, e.getMessage());
            return host;
        }
    }

    private boolean isWorkerHealthy(Node node) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(node.getHost(), config.getBridgePort()), config.getWorkerHealthCheckTimeoutMs());
            return true;
        } catch (Exception e) {
            log.warn("Worker %s bridge server is unhealthy: %s", node.getHost(), e.getMessage());
            return false;
        }
    }
}
