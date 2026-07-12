package io.trino.plugin.teradata.export;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Map;

/**
 * Represents a split for the Teradata Export connector.
 * 
 * IMPORTANT for multi-worker setups:
 * - Each split is assigned to a specific worker based on workerHost
 * - isRemotelyAccessible() returns FALSE to enforce local execution
 * - This ensures data sent to Worker N is processed by Worker N's PageSource
 */
public class TrinoExportSplit implements ConnectorSplit {
    private final String workerHost;
    private final String queryId;
    private final String targetIps;
    private final String token;

    @JsonCreator
    public TrinoExportSplit(
            @JsonProperty("workerHost") String workerHost,
            @JsonProperty("queryId") String queryId,
            @JsonProperty("targetIps") String targetIps,
            @JsonProperty("token") String token) {
        this.workerHost = workerHost;
        this.queryId = queryId;
        this.targetIps = targetIps;
        this.token = token;
    }

    @JsonProperty
    public String getWorkerHost() {
        return workerHost;
    }

    @JsonProperty
    public String getQueryId() {
        return queryId;
    }

    @JsonProperty
    public String getTargetIps() {
        return targetIps;
    }

    @JsonProperty
    public String getToken() {
        return token;
    }

    /**
     * CRITICAL: Return false to enforce split locality.
     * This ensures the split runs on the worker specified by getAddresses(),
     * which is where the corresponding Teradata AMPs will send data.
     * 
     * If this returns true, Trino optimizer might schedule the split on
     * a different worker, causing data routing mismatch.
     */
    @Override
    public boolean isRemotelyAccessible() {
        return false;  // Enforce local execution for data locality
    }

    @Override
    public List<HostAddress> getAddresses() {
        return List.of(HostAddress.fromString(workerHost));
    }

    public Map<String, String> getSplitInfo() {
        return Map.of(
            "workerHost", workerHost,
            "queryId", queryId,
            "targetIps", targetIps,
            "token", token
        );
    }
}
