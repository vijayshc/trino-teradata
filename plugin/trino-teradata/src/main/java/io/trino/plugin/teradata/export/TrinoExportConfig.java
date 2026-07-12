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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.Duration;
import java.util.concurrent.TimeUnit;

public class TrinoExportConfig {
    // === Core Connection Settings ===
    private String teradataUrl;
    private String teradataUser;
    private String teradataPassword;
    private String passwordScript;
    private String teradataTimezone = "-05:00";

    // === Network/Port Settings ===
    private int flightPort = 50051;
    private int bridgePort = 9999;
    private String trinoAddress = "localhost";
    private String flightBindAddress = "0.0.0.0";
    private String workerAdvertisedAddresses;  // For NAT/multi-homed networks

    // === Buffer/Performance Settings ===
    // Option G: Increase Batch Size Dramatically
    private int batchSize = 500000;  // 5x larger batches reduce per-batch overhead
    private int socketReceiveBufferSize = 128 * 1024 * 1024;  // 128MB for high-bandwidth networks
    private int inputBufferSize = 16 * 1024 * 1024;           // 16MB
    private int bufferQueueCapacity = 500;  // Higher capacity for burst tolerance
    private long pagePollTimeoutMs = 100;   // More aggressive polling
    private int splitsPerWorker = 8;        // Match available CPU cores, not AMPs
    private boolean compressionEnabled = true;  // Enable by default for max throughput
    private CompressionAlgorithm compressionAlgorithm = CompressionAlgorithm.ZLIB;
    
    public enum CompressionAlgorithm {
        ZLIB, LZ4
    }


    // === Dynamic Filtering Settings ===
    private Duration dynamicFilterTimeout = new Duration(10, TimeUnit.SECONDS);
    private boolean enableDynamicFiltering = true;
    private boolean enableAggregationPushdown = true;
    private boolean enableTopNPushdown = true;
    private boolean systemQueryEnabled = true;
    private int systemQueryMaxLength = 1_048_576;

    private boolean enforceProxyAuthentication = true;

    // === UDF Settings ===
    private String udfDatabase = "TrinoExport";
    private String udfName = "ExportToTrino";

    // === Schema Settings ===
    private String defaultSchemas = "TrinoExport,default";

    // === Logging Settings ===
    private boolean enableDebugLogging = false;

    // === Scalability Settings ===
    private int maxQueryConcurrency = 50;
    private int maxBridgeThreads = 200;
    private int bridgeCorePoolSize = 10;
    private int bridgeQueueCapacity = 500;

    // === Cache Settings ===
    private int metadataCacheSize = 1000;
    private int bridgeSocketTimeoutMs = 60000;
    private int broadcastSocketTimeoutMs = 5000;

    // === Health Check Settings ===
    private boolean workerHealthCheckEnabled = true;
    private int workerHealthCheckTimeoutMs = 2000;

    // === Metadata Refresh Settings ===
    private Duration metadataRefreshInterval = new Duration(0, TimeUnit.MINUTES); // 0 = disabled
    private String metadataRefreshSchemas = ""; // comma-separated list
    private boolean metadataRefreshColumns = true;

    // === Pushdown Settings (Session Property Defaults) ===
    private boolean enableJoinPushdown = true;
    private boolean enableComplexJoinPushdown = true;
    private boolean enableComplexExpressionPushdown = true;
    private int domainCompactionThreshold = 100;

    // === Connection Pool Settings ===
    private int connectionPoolMinSize = 5;
    private int connectionPoolMaxSize = 20;
    private long connectionPoolMaxIdleMs = 300000L;  // 5 minutes

    // === Executor Thread Pool Settings ===
    private int executorCorePoolSize = 20;  // Core threads for split executor

    // ============================================================
    // Core Connection Getters/Setters
    // ============================================================

    public String getTeradataUrl() {
        return teradataUrl;
    }

    @Config("teradata.url")
    public TrinoExportConfig setTeradataUrl(String teradataUrl) {
        this.teradataUrl = teradataUrl;
        return this;
    }

    public String getTeradataUser() {
        return teradataUser;
    }

    @Config("teradata.user")
    public TrinoExportConfig setTeradataUser(String teradataUser) {
        this.teradataUser = teradataUser;
        return this;
    }

    @Config("teradata.password")
    @ConfigSecuritySensitive
    public TrinoExportConfig setTeradataPassword(String teradataPassword) {
        this.teradataPassword = teradataPassword;
        return this;
    }

    @Config("teradata.password-script")
    public TrinoExportConfig setPasswordScript(String passwordScript) {
        this.passwordScript = passwordScript;
        return this;
    }

    public String getPasswordScript() {
        return passwordScript;
    }

    public String getTeradataPassword() {
        if (passwordScript != null && !passwordScript.isEmpty()) {
            return executeScript(passwordScript);
        }
        return teradataPassword;
    }

    public String getTeradataTimezone() {
        return teradataTimezone;
    }

    @Config("teradata.timezone")
    public TrinoExportConfig setTeradataTimezone(String teradataTimezone) {
        this.teradataTimezone = teradataTimezone;
        return this;
    }

    // ============================================================
    // Network/Port Getters/Setters
    // ============================================================

    public int getFlightPort() {
        return flightPort;
    }

    @Config("teradata.export.flight-port")
    public TrinoExportConfig setFlightPort(int flightPort) {
        this.flightPort = flightPort;
        return this;
    }

    public int getBridgePort() {
        return bridgePort;
    }

    @Config("teradata.export.bridge-port")
    public TrinoExportConfig setBridgePort(int bridgePort) {
        this.bridgePort = bridgePort;
        return this;
    }

    public String getTrinoAddress() {
        return trinoAddress;
    }

    @Config("teradata.export.trino-address")
    public TrinoExportConfig setTrinoAddress(String trinoAddress) {
        this.trinoAddress = trinoAddress;
        return this;
    }

    public String getFlightBindAddress() {
        return flightBindAddress;
    }

    @Config("teradata.export.flight-bind-address")
    public TrinoExportConfig setFlightBindAddress(String flightBindAddress) {
        this.flightBindAddress = flightBindAddress;
        return this;
    }

    public String getWorkerAdvertisedAddresses() {
        return workerAdvertisedAddresses;
    }

    @Config("teradata.export.worker-advertised-addresses")
    public TrinoExportConfig setWorkerAdvertisedAddresses(String workerAdvertisedAddresses) {
        this.workerAdvertisedAddresses = workerAdvertisedAddresses;
        return this;
    }

    // ============================================================
    // Buffer/Performance Getters/Setters
    // ============================================================

    public int getBatchSize() {
        return batchSize;
    }
    
    @Config("teradata.export.batch-size")
    public TrinoExportConfig setBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public int getSocketReceiveBufferSize() {
        return socketReceiveBufferSize;
    }

    @Config("teradata.export.socket-receive-buffer-size")
    public TrinoExportConfig setSocketReceiveBufferSize(int socketReceiveBufferSize) {
        this.socketReceiveBufferSize = socketReceiveBufferSize;
        return this;
    }

    public int getInputBufferSize() {
        return inputBufferSize;
    }

    @Config("teradata.export.input-buffer-size")
    public TrinoExportConfig setInputBufferSize(int inputBufferSize) {
        this.inputBufferSize = inputBufferSize;
        return this;
    }

    public int getBufferQueueCapacity() {
        return bufferQueueCapacity;
    }

    @Config("teradata.export.buffer-queue-capacity")
    public TrinoExportConfig setBufferQueueCapacity(int bufferQueueCapacity) {
        this.bufferQueueCapacity = bufferQueueCapacity;
        return this;
    }

    public long getPagePollTimeoutMs() {
        return pagePollTimeoutMs;
    }

    @Config("teradata.export.page-poll-timeout-ms")
    public TrinoExportConfig setPagePollTimeoutMs(long pagePollTimeoutMs) {
        this.pagePollTimeoutMs = pagePollTimeoutMs;
        return this;
    }

    public int getSplitsPerWorker() {
        return splitsPerWorker;
    }

    @Config("teradata.export.splits-per-worker")
    public TrinoExportConfig setSplitsPerWorker(int splitsPerWorker) {
        this.splitsPerWorker = splitsPerWorker;
        return this;
    }

    public boolean isCompressionEnabled() {
        return compressionEnabled;
    }

    @Config("teradata.export.compression-enabled")
    public TrinoExportConfig setCompressionEnabled(boolean compressionEnabled) {
        this.compressionEnabled = compressionEnabled;
        return this;
    }

    public CompressionAlgorithm getCompressionAlgorithm() {
        return compressionAlgorithm;
    }

    @Config("teradata.export.compression-algorithm")
    public TrinoExportConfig setCompressionAlgorithm(CompressionAlgorithm compressionAlgorithm) {
        this.compressionAlgorithm = compressionAlgorithm;
        return this;
    }

    // ============================================================
    // Dynamic Filtering Getters/Setters
    // ============================================================

    public Duration getDynamicFilterTimeout() {
        return dynamicFilterTimeout;
    }

    @Config("teradata.export.dynamic-filter-timeout")
    public TrinoExportConfig setDynamicFilterTimeout(Duration dynamicFilterTimeout) {
        this.dynamicFilterTimeout = dynamicFilterTimeout;
        return this;
    }

    public boolean isEnableDynamicFiltering() {
        return enableDynamicFiltering;
    }

    @Config("teradata.export.enable-dynamic-filtering")
    public TrinoExportConfig setEnableDynamicFiltering(boolean enableDynamicFiltering) {
        this.enableDynamicFiltering = enableDynamicFiltering;
        return this;
    }

    public boolean isEnableAggregationPushdown() {
        return enableAggregationPushdown;
    }

    @Config("teradata.export.enable-aggregation-pushdown")
    public TrinoExportConfig setEnableAggregationPushdown(boolean enableAggregationPushdown) {
        this.enableAggregationPushdown = enableAggregationPushdown;
        return this;
    }

    public boolean isEnableTopNPushdown() {
        return enableTopNPushdown;
    }

    @Config("teradata.export.enable-topn-pushdown")
    public TrinoExportConfig setEnableTopNPushdown(boolean enableTopNPushdown) {
        this.enableTopNPushdown = enableTopNPushdown;
        return this;
    }

    public boolean isSystemQueryEnabled() {
        return systemQueryEnabled;
    }

    @Config("teradata.export.system-query-enabled")
    @ConfigDescription("Enable system.query table function for full query pushdown")
    public TrinoExportConfig setSystemQueryEnabled(boolean systemQueryEnabled) {
        this.systemQueryEnabled = systemQueryEnabled;
        return this;
    }

    public int getSystemQueryMaxLength() {
        return systemQueryMaxLength;
    }

    @Config("teradata.export.system-query-max-length")
    @ConfigDescription("Maximum allowed length of the SQL text accepted by system.query")
    public TrinoExportConfig setSystemQueryMaxLength(int systemQueryMaxLength) {
        this.systemQueryMaxLength = systemQueryMaxLength;
        return this;
    }

    // ============================================================
    // Security Getters/Setters
    // ============================================================

    public boolean isEnforceProxyAuthentication() {
        return enforceProxyAuthentication;
    }

    @Config("teradata.export.enforce-proxy-authentication")
    public TrinoExportConfig setEnforceProxyAuthentication(boolean enforceProxyAuthentication) {
        this.enforceProxyAuthentication = enforceProxyAuthentication;
        return this;
    }

    // ============================================================
    // UDF Settings Getters/Setters
    // ============================================================

    public String getUdfDatabase() {
        return udfDatabase;
    }

    @Config("teradata.export.udf-database")
    public TrinoExportConfig setUdfDatabase(String udfDatabase) {
        this.udfDatabase = udfDatabase;
        return this;
    }

    public String getUdfName() {
        return udfName;
    }

    @Config("teradata.export.udf-name")
    public TrinoExportConfig setUdfName(String udfName) {
        this.udfName = udfName;
        return this;
    }

    // ============================================================
    // Schema Settings Getters/Setters
    // ============================================================

    public String getDefaultSchemas() {
        return defaultSchemas;
    }

    @Config("teradata.export.default-schemas")
    public TrinoExportConfig setDefaultSchemas(String defaultSchemas) {
        this.defaultSchemas = defaultSchemas;
        return this;
    }

    public String[] getDefaultSchemasArray() {
        if (defaultSchemas == null || defaultSchemas.isEmpty()) {
            return new String[0];
        }
        return defaultSchemas.split(",");
    }

    // ============================================================
    // Logging Settings Getters/Setters
    // ============================================================

    public boolean isEnableDebugLogging() {
        return enableDebugLogging;
    }

    @Config("teradata.export.enable-debug-logging")
    public TrinoExportConfig setEnableDebugLogging(boolean enableDebugLogging) {
        this.enableDebugLogging = enableDebugLogging;
        return this;
    }

    // ============================================================
    // Scalability Getters/Setters
    // ============================================================

    public int getMaxQueryConcurrency() {
        return maxQueryConcurrency;
    }

    @Config("teradata.export.max-query-concurrency")
    public TrinoExportConfig setMaxQueryConcurrency(int maxQueryConcurrency) {
        this.maxQueryConcurrency = maxQueryConcurrency;
        return this;
    }

    public int getMaxBridgeThreads() {
        return maxBridgeThreads;
    }

    @Config("teradata.export.max-bridge-threads")
    public TrinoExportConfig setMaxBridgeThreads(int maxBridgeThreads) {
        this.maxBridgeThreads = maxBridgeThreads;
        return this;
    }

    public int getBridgeQueueCapacity() {
        return bridgeQueueCapacity;
    }

    @Config("teradata.export.bridge-queue-capacity")
    public TrinoExportConfig setBridgeQueueCapacity(int bridgeQueueCapacity) {
        this.bridgeQueueCapacity = bridgeQueueCapacity;
        return this;
    }

    public int getBridgeCorePoolSize() {
        return bridgeCorePoolSize;
    }

    @Config("teradata.export.bridge-core-pool-size")
    @ConfigDescription("Core thread pool size for bridge server")
    public TrinoExportConfig setBridgeCorePoolSize(int bridgeCorePoolSize) {
        this.bridgeCorePoolSize = bridgeCorePoolSize;
        return this;
    }

    // ============================================================
    // Cache Settings Getters/Setters
    // ============================================================

    @Config("teradata.export.metadata-cache-size")
    @ConfigDescription("Maximum number of entries in metadata cache")
    public TrinoExportConfig setMetadataCacheSize(int metadataCacheSize) {
        this.metadataCacheSize = metadataCacheSize;
        return this;
    }

    public int getMetadataCacheSize() {
        return metadataCacheSize;
    }

    @Config("teradata.export.bridge-socket-timeout-ms")
    @ConfigDescription("Socket read timeout for bridge server data connections")
    public TrinoExportConfig setBridgeSocketTimeoutMs(int bridgeSocketTimeoutMs) {
        this.bridgeSocketTimeoutMs = bridgeSocketTimeoutMs;
        return this;
    }

    public int getBridgeSocketTimeoutMs() {
        return bridgeSocketTimeoutMs;
    }

    public int getBroadcastSocketTimeoutMs() {
        return broadcastSocketTimeoutMs;
    }

    @Config("teradata.export.broadcast-socket-timeout-ms")
    @ConfigDescription("Socket timeout for broadcasting finished signal")
    public TrinoExportConfig setBroadcastSocketTimeoutMs(int broadcastSocketTimeoutMs) {
        this.broadcastSocketTimeoutMs = broadcastSocketTimeoutMs;
        return this;
    }

    @Config("teradata.export.worker-health-check-enabled")
    @ConfigDescription("Enable health check for worker nodes before assigning splits")
    public TrinoExportConfig setWorkerHealthCheckEnabled(boolean workerHealthCheckEnabled) {
        this.workerHealthCheckEnabled = workerHealthCheckEnabled;
        return this;
    }

    public boolean isWorkerHealthCheckEnabled() {
        return workerHealthCheckEnabled;
    }

    @Config("teradata.export.worker-health-check-timeout-ms")
    @ConfigDescription("Timeout for worker health check (socket connect)")
    public TrinoExportConfig setWorkerHealthCheckTimeoutMs(int workerHealthCheckTimeoutMs) {
        this.workerHealthCheckTimeoutMs = workerHealthCheckTimeoutMs;
        return this;
    }

    public int getWorkerHealthCheckTimeoutMs() {
        return workerHealthCheckTimeoutMs;
    }

    // ============================================================
    // Pushdown Settings Getters/Setters (Session Property Defaults)
    // ============================================================

    public boolean isEnableJoinPushdown() {
        return enableJoinPushdown;
    }

    @Config("teradata.export.enable-join-pushdown")
    public TrinoExportConfig setEnableJoinPushdown(boolean enableJoinPushdown) {
        this.enableJoinPushdown = enableJoinPushdown;
        return this;
    }

    public boolean isEnableComplexJoinPushdown() {
        return enableComplexJoinPushdown;
    }

    @Config("teradata.export.enable-complex-join-pushdown")
    public TrinoExportConfig setEnableComplexJoinPushdown(boolean enableComplexJoinPushdown) {
        this.enableComplexJoinPushdown = enableComplexJoinPushdown;
        return this;
    }

    public boolean isEnableComplexExpressionPushdown() {
        return enableComplexExpressionPushdown;
    }

    @Config("teradata.export.enable-complex-expression-pushdown")
    public TrinoExportConfig setEnableComplexExpressionPushdown(boolean enableComplexExpressionPushdown) {
        this.enableComplexExpressionPushdown = enableComplexExpressionPushdown;
        return this;
    }

    public int getDomainCompactionThreshold() {
        return domainCompactionThreshold;
    }

    @Config("teradata.export.domain-compaction-threshold")
    public TrinoExportConfig setDomainCompactionThreshold(int domainCompactionThreshold) {
        this.domainCompactionThreshold = domainCompactionThreshold;
        return this;
    }

    // ============================================================
    // Metadata Refresh Getters/Setters
    // ============================================================

    public Duration getMetadataRefreshInterval() {
        return metadataRefreshInterval;
    }

    @Config("teradata.export.metadata-refresh-interval")
    public TrinoExportConfig setMetadataRefreshInterval(Duration metadataRefreshInterval) {
        this.metadataRefreshInterval = metadataRefreshInterval;
        return this;
    }

    public String getMetadataRefreshSchemas() {
        return metadataRefreshSchemas;
    }

    @Config("teradata.export.metadata-refresh-schemas")
    public TrinoExportConfig setMetadataRefreshSchemas(String metadataRefreshSchemas) {
        this.metadataRefreshSchemas = metadataRefreshSchemas;
        return this;
    }

    public String[] getMetadataRefreshSchemasArray() {
        if (metadataRefreshSchemas == null || metadataRefreshSchemas.isEmpty()) {
            return new String[0];
        }
        return metadataRefreshSchemas.split(",");
    }

    public boolean isMetadataRefreshColumns() {
        return metadataRefreshColumns;
    }

    @Config("teradata.export.metadata-refresh-columns")
    public TrinoExportConfig setMetadataRefreshColumns(boolean metadataRefreshColumns) {
        this.metadataRefreshColumns = metadataRefreshColumns;
        return this;
    }

    // ============================================================
    // Connection Pool Getters/Setters
    // ============================================================

    public int getConnectionPoolMinSize() {
        return connectionPoolMinSize;
    }

    @Config("teradata.export.connection-pool-min-size")
    @ConfigDescription("Minimum connections per user in the pool")
    public TrinoExportConfig setConnectionPoolMinSize(int connectionPoolMinSize) {
        this.connectionPoolMinSize = connectionPoolMinSize;
        return this;
    }

    public int getConnectionPoolMaxSize() {
        return connectionPoolMaxSize;
    }

    @Config("teradata.export.connection-pool-max-size")
    @ConfigDescription("Maximum connections per user in the pool")
    public TrinoExportConfig setConnectionPoolMaxSize(int connectionPoolMaxSize) {
        this.connectionPoolMaxSize = connectionPoolMaxSize;
        return this;
    }

    public long getConnectionPoolMaxIdleMs() {
        return connectionPoolMaxIdleMs;
    }

    @Config("teradata.export.connection-pool-max-idle-ms")
    @ConfigDescription("Maximum idle time for pooled connections in milliseconds")
    public TrinoExportConfig setConnectionPoolMaxIdleMs(long connectionPoolMaxIdleMs) {
        this.connectionPoolMaxIdleMs = connectionPoolMaxIdleMs;
        return this;
    }

    // ============================================================
    // Executor Thread Pool Getters/Setters
    // ============================================================

    public int getExecutorCorePoolSize() {
        return executorCorePoolSize;
    }

    @Config("teradata.export.executor-core-pool-size")
    @ConfigDescription("Core thread pool size for split executor")
    public TrinoExportConfig setExecutorCorePoolSize(int executorCorePoolSize) {
        this.executorCorePoolSize = executorCorePoolSize;
        return this;
    }

    // ============================================================
    // Utility Methods
    // ============================================================

    private String executeScript(String scriptPath) {
        try {
            Process process = Runtime.getRuntime().exec(new String[]{"/bin/bash", "-c", scriptPath});
            try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(process.getInputStream()))) {
                String output = reader.readLine();
                if (output != null) {
                    return output.trim();
                }
            }
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                io.airlift.log.Logger.get(TrinoExportConfig.class).error("Script %s exited with code %d", scriptPath, exitCode);
            }
        } catch (Exception e) {
            io.airlift.log.Logger.get(TrinoExportConfig.class).error(e, "Error executing script %s", scriptPath);
        }
        return null;
    }
}
