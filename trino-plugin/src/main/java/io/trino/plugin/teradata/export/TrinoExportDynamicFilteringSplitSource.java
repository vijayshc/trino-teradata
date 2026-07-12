package io.trino.plugin.teradata.export;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.connector.*;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.ColumnHandle;

import io.trino.plugin.jdbc.BooleanWriteFunction;
import io.trino.plugin.jdbc.DoubleWriteFunction;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.SliceWriteFunction;
import io.airlift.slice.Slice;

import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Custom SplitSource that uses Trino's JDBC infrastructure for ALL SQL generation.
 */
public class TrinoExportDynamicFilteringSplitSource implements ConnectorSplitSource {
    private static final Logger log = Logger.get(TrinoExportDynamicFilteringSplitSource.class);

    private final List<ConnectorSplit> splits;
    private final DynamicFilter dynamicFilter;
    private final JdbcTableHandle tableHandle;
    private final String splitId;
    private final String targetIps;
    private final String dynamicToken;
    private final String trinoUser;
    private final TrinoExportConfig config;
    private final ExecutorService executor;
    private final TeradataClient teradataClient;
    private final TeradataQueryBuilder queryBuilder;
    private final ConnectorSession session;
    private final TeradataConnectionPool connectionPool;
    
    private final AtomicBoolean teradataExecutionStarted = new AtomicBoolean(false);
    private final AtomicBoolean splitsReturned = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public TrinoExportDynamicFilteringSplitSource(
            List<ConnectorSplit> splits,
            DynamicFilter dynamicFilter,
            JdbcTableHandle tableHandle,
            String splitId,
            String targetIps,
            String dynamicToken,
            String trinoUser,
            TrinoExportConfig config,
            ExecutorService executor,
            TeradataClient teradataClient,
            TeradataQueryBuilder queryBuilder,
            ConnectorSession session,
            TeradataConnectionPool connectionPool) {
        this.splits = new ArrayList<>(splits);
        this.dynamicFilter = dynamicFilter;
        this.tableHandle = tableHandle;
        this.splitId = splitId;
        this.targetIps = targetIps;
        this.dynamicToken = dynamicToken;
        this.trinoUser = trinoUser;
        this.config = config;
        this.executor = executor;
        this.teradataClient = teradataClient;
        this.queryBuilder = queryBuilder;
        this.session = session;
        this.connectionPool = connectionPool;
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize) {
        if (closed.get() || splitsReturned.get()) {
            return CompletableFuture.completedFuture(new ConnectorSplitBatch(List.of(), true));
        }

        if (config.isEnableDynamicFiltering() && dynamicFilter != null && dynamicFilter.isAwaitable()) {
            if (!dynamicFilter.isComplete()) {
                log.info("Waiting for dynamic filter for query %s", splitId);
                CompletableFuture<?> blocked = dynamicFilter.isBlocked().toCompletableFuture();
                return blocked.thenApply(v -> createSplitBatchWithTeradataExecution());
            }
        }

        return CompletableFuture.completedFuture(createSplitBatchWithTeradataExecution());
    }

    private ConnectorSplitBatch createSplitBatchWithTeradataExecution() {
        if (teradataExecutionStarted.compareAndSet(false, true)) {
            log.info("Triggering Teradata execution for query %s", splitId);
            executor.submit(this::triggerTeradataExecution);
        }
        splitsReturned.set(true);
        return new ConnectorSplitBatch(splits, true);
    }

    private void triggerTeradataExecution() {
        List<String> targetList = parseTargetList(targetIps);
        try (Connection conn = getConnection()) {
            // Generate SQL using Trino's DefaultQueryBuilder.prepareSelectQuery()
            PreparedQuery innerQuery = generateSqlViaTrinoInfrastructure(conn);
            log.info("Generated SQL via Trino JDBC for query %s: %s", splitId, innerQuery.query());
            
            String compressionAlgorithmName = config.isCompressionEnabled() ? 
                    config.getCompressionAlgorithm().name() : "NONE";

            // ONLY Teradata-specific addition: wrap with Table Operator UDF
            // Pass limit and sortOrder for TOP/SAMPLE wrapping
            String teradataSql = queryBuilder.buildExportQuery(
                    innerQuery.query(),
                    config.getUdfDatabase(),
                    config.getUdfName(),
                    targetIps,
                    splitId,
                    dynamicToken,
                    config.getBatchSize(),
                    compressionAlgorithmName,
                    tableHandle.getLimit(),
                    tableHandle.getSortOrder());


            String logSql = teradataSql.replace(dynamicToken, "***DYNAMIC_TOKEN***");
            log.info("Executing Teradata SQL for query %s: %s", splitId, logSql);

            if (!targetList.isEmpty()) {
                broadcastRegisterToken(targetList, dynamicToken);
            }

            try (java.sql.PreparedStatement stmt = conn.prepareStatement(teradataSql)) {
                // Bind parameters from the inner query
                List<io.trino.plugin.jdbc.QueryParameter> params = innerQuery.parameters();
                
                StringBuilder paramLog = new StringBuilder("Query Parameters: ");
                for (io.trino.plugin.jdbc.QueryParameter p : params) {
                    Object val = p.getValue().orElse(null);
                    if (val instanceof Slice) {
                        paramLog.append("[").append(((Slice) val).toStringUtf8()).append("] ");
                    } else {
                        paramLog.append("[").append(val).append("] ");
                    }
                }
                log.info(paramLog.toString());
                
                for (int i = 0; i < params.size(); i++) {
                    io.trino.plugin.jdbc.QueryParameter param = params.get(i);
                    io.trino.plugin.jdbc.WriteMapping writeMapping = teradataClient.toWriteMapping(session, param.getType());
                    io.trino.plugin.jdbc.WriteFunction writeFunction = writeMapping.getWriteFunction();
                    Object value = param.getValue().orElse(null);
                    
                    if (value == null) {
                        writeFunction.setNull(stmt, i + 1);
                    } else {
                        try {
                            if (writeFunction instanceof LongWriteFunction) {
                                ((LongWriteFunction) writeFunction).set(stmt, i + 1, ((Number) value).longValue());
                            } else if (writeFunction instanceof DoubleWriteFunction) {
                                ((DoubleWriteFunction) writeFunction).set(stmt, i + 1, ((Number) value).doubleValue());
                            } else if (writeFunction instanceof BooleanWriteFunction) {
                                ((BooleanWriteFunction) writeFunction).set(stmt, i + 1, (Boolean) value);
                            } else if (writeFunction instanceof SliceWriteFunction) {
                                ((SliceWriteFunction) writeFunction).set(stmt, i + 1, (Slice) value);
                            } else if (writeFunction instanceof ObjectWriteFunction) {
                                ((ObjectWriteFunction) writeFunction).set(stmt, i + 1, value);
                            } else {
                                log.warn("Unknown WriteFunction type %s, attempting generic setObject", writeFunction.getClass().getName());
                                stmt.setObject(i + 1, value);
                            }
                        } catch (ClassCastException cce) {
                            log.error("Failed to cast value %s (type %s) for WriteFunction %s", value, value.getClass().getName(), writeFunction.getClass().getName());
                            throw cce;
                        }
                    }
                }

                log.info("Executing query for %s", splitId);
                try (java.sql.ResultSet rs = stmt.executeQuery()) {
                    log.info("Query executed, processing result set for %s", splitId);
                    
                    // DETERMINISTIC EOS: Collect AMP IDs for per-worker expected counts.
                    // Each AMP sends TERADATA_FINISHED signal after data - no global coordination needed.
                    List<Integer> ampIds = new ArrayList<>();
                    while (rs.next()) {
                        int ampId = rs.getInt(1);  // Column 1 = AMP unique ID from FNC_TblOpGetUniqID
                        ampIds.add(ampId);
                        String errorMsg = rs.getString(7);
                        if (errorMsg != null && (errorMsg.startsWith("ERROR") || errorMsg.contains("failed"))) {
                            log.error("Teradata UDF reported error: %s", errorMsg);
                            throw new RuntimeException("Teradata UDF execution failed: " + errorMsg);
                        }
                    }

                    int rowCount = ampIds.size();
                    
                    // DETERMINISTIC EOS: Broadcast expected counts per worker (with retries for reliability)
                    if (rowCount == 0) {
                        // TRUE 0-row case: no AMPs executed because input was empty.
                        log.info("Teradata UDF returned 0 rows for query %s. Marking as true 0-row case.", splitId);
                        DataBufferRegistry.setExpectedTeradataSignals(splitId, 0);
                        if (!targetList.isEmpty()) {
                            broadcastExpectedConnectionsPerWorker(targetList, ampIds, dynamicToken);
                        }
                    } else {
                        // Normal case: Calculate and broadcast expected connections per worker
                        log.info("Teradata SQL execution finished for query %s (AMPs: %d). Broadcasting expected signals.", splitId, rowCount);
                        if (!targetList.isEmpty()) {
                            broadcastExpectedConnectionsPerWorker(targetList, ampIds, dynamicToken);
                        }
                    }
                    
                    log.info("Teradata SQL execution finished successfully for query %s (Rows: %d)", splitId, rowCount);
                }
            }
        } catch (Exception e) {
            log.error(e, "Error executing Teradata SQL for query %s", splitId);
            // On error, broadcast 0 expected to unblock waiting workers
            DataBufferRegistry.setExpectedTeradataSignals(splitId, 0);
            if (!targetList.isEmpty()) {
                broadcastExpectedConnectionsPerWorker(targetList, new ArrayList<>(), dynamicToken);
            }
            DataBufferRegistry.cleanupOnFailure(splitId);
            throw new RuntimeException("Teradata query execution failed for " + splitId + ": " + e.getMessage(), e);
        }
    }

    private void bindParameter(java.sql.PreparedStatement stmt, int index, io.trino.plugin.jdbc.WriteFunction writeFunction, Object value) throws SQLException {
        // Since we can't easily cast to specific WriteFunction types without seeing them, 
        // and standard WriteFunction interface might use a generic set or we have to rely on reflection/casting.
        // HOWEVER, standard java.sql.PreparedStatement.setObject works for most things if the driver handles it.
        // Trino's WriteFunction logic is usually: ((LongWriteFunction) writeFunction).set(stmt, index, (long) value)
        
        // As a workaround for now, we will use generic setObject if we can't invoke specific writeFunction.
        // But let's try to assume we can call the method or just use setObject which Teradata driver supports well.
        Class<?> javaType = writeFunction.getJavaType();
        if (javaType == boolean.class) {
            stmt.setBoolean(index, (Boolean) value);
        } else if (javaType == long.class) {
            stmt.setLong(index, (Long) value);
        } else if (javaType == double.class) {
            stmt.setDouble(index, (Double) value);
        } else if (javaType == String.class) {
             stmt.setString(index, (String) value);
        } else {
             stmt.setObject(index, value);
        }
    }

    private PreparedQuery generateSqlViaTrinoInfrastructure(Connection conn) {
        // Get columns from JdbcTableHandle
        List<JdbcColumnHandle> columns = tableHandle.getColumns().filter(list -> !list.isEmpty()).orElseGet(() -> {
            log.info("No columns in table handle (or empty list), fetching all columns for %s", tableHandle);
            if (tableHandle.isNamedRelation()) {
                SchemaTableName schemaTableName = tableHandle.getRequiredNamedRelation().getSchemaTableName();
                RemoteTableName remoteTableName = tableHandle.getRequiredNamedRelation().getRemoteTableName();
                return teradataClient.getColumns(session, schemaTableName, remoteTableName);
            }
            throw new RuntimeException("No columns in table handle and not a named relation. Synthetic query handles must preserve analyzed output columns: " + tableHandle);
        });
        
        // Get constraint (predicate pushdown)
        TupleDomain<ColumnHandle> constraint = tableHandle.getConstraint();
        
        // Add dynamic filter predicates
        if (config.isEnableDynamicFiltering() && dynamicFilter != null) {
            TupleDomain<ColumnHandle> dynamicPredicate = dynamicFilter.getCurrentPredicate();
            if (!dynamicPredicate.isAll()) {
                constraint = constraint.intersect(dynamicPredicate);
            }
        }
        
        // Get grouping sets for aggregation
        Optional<List<List<JdbcColumnHandle>>> groupingSets = Optional.empty();
        
        // Get constraint expressions (complex predicates)
        List<ParameterizedExpression> constraintExpressions = tableHandle.getConstraintExpressions();
        Optional<ParameterizedExpression> additionalPredicate = constraintExpressions.isEmpty() 
                ? Optional.empty() 
                : Optional.of(combineExpressions(constraintExpressions));
        
        Map<String, ParameterizedExpression> columnExpressions = new HashMap<>();
        Set<String> constrainedColumns = new HashSet<>();
        constraint.getDomains().ifPresent(domains -> domains.keySet().forEach(columnHandle -> {
            if (columnHandle instanceof JdbcColumnHandle jdbcColumnHandle) {
                constrainedColumns.add(jdbcColumnHandle.getColumnName());
            }
        }));

        for (JdbcColumnHandle column : columns) {
            if (constrainedColumns.contains(column.getColumnName())) {
                continue;
            }

            teradataClient.getColumnExpressionForPushdown(column)
                    .ifPresent(expression -> columnExpressions.put(column.getColumnName(), expression));
        }

        if (!columnExpressions.isEmpty()) {
            log.info("Applying %d projection rewrite(s) for query %s: %s",
                    columnExpressions.size(), splitId, columnExpressions.keySet());
        }
        
        // Call DefaultQueryBuilder.prepareSelectQuery()
        PreparedQuery preparedQuery = queryBuilder.prepareSelectQuery(
                teradataClient,
                session,
                conn,
                tableHandle.getRelationHandle(),
                groupingSets,
                columns,
                columnExpressions,
                constraint,
                additionalPredicate);
        
        return preparedQuery;
    }

    private ParameterizedExpression combineExpressions(List<ParameterizedExpression> expressions) {
        if (expressions.size() == 1) {
            return expressions.get(0);
        }
        
        StringBuilder combined = new StringBuilder();
        List<io.trino.plugin.jdbc.QueryParameter> allParams = new ArrayList<>();
        
        for (int i = 0; i < expressions.size(); i++) {
            if (i > 0) {
                combined.append(" AND ");
            }
            combined.append("(").append(expressions.get(i).expression()).append(")");
            allParams.addAll(expressions.get(i).parameters());
        }
        
        return new ParameterizedExpression(combined.toString(), allParams);
    }

    /**
     * DETERMINISTIC EOS: Broadcast expected connections per worker using deterministic routing.
     * 
     * With deterministic routing (amp_id % num_workers), we know exactly which worker
     * each AMP will connect to. Each worker receives its specific expected count.
     * 
     * Single attempt with short timeout - local bridge servers should always be reachable.
     * If broadcast fails, query will timeout via PageSource poll timeout.
     */
    private void broadcastExpectedConnectionsPerWorker(List<String> targets, List<Integer> ampIds, String dynamicToken) {
        if (targets == null || targets.isEmpty()) return;
        
        int numWorkers = targets.size();
        
        // Calculate expected connections per worker using deterministic routing.
        // C UDF: assigned_worker_idx = ((unsigned)routing_amp_id) % worker_count
        // and returns routing_amp_id as result column 1. Match with unsigned modulo.
        int[] expectedPerWorker = new int[numWorkers];
        StringBuilder ampDebug = new StringBuilder();
        for (int ampId : ampIds) {
            int workerIdx = (int) (Integer.toUnsignedLong(ampId) % numWorkers);
            expectedPerWorker[workerIdx]++;
            ampDebug.append(String.format("PID%d->w%d ", ampId, workerIdx));
        }
        
        log.info("DETERMINISTIC EOS: %d AMPs distributed across %d workers: %s (Details: %s)", 
                ampIds.size(), numWorkers, java.util.Arrays.toString(expectedPerWorker), ampDebug.toString().trim());
        
        // Broadcast specific expected count to each worker (no retries - local servers should respond quickly)
        for (int i = 0; i < targets.size(); i++) {
            String target = targets.get(i);
            if (target == null || target.isEmpty()) continue;
            
            int expected = expectedPerWorker[i];
            
            try {
                String[] parts = target.split(":");
                String host = parts[0];
                int port = Integer.parseInt(parts[1]);
                
                try (Socket socket = new Socket()) {
                    // Use connect timeout of 1 second for local server
                    socket.connect(new java.net.InetSocketAddress(host, port), 1000);
                    socket.setSoTimeout(1000);  // Read timeout 1 second
                    
                    DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                    
                    if (dynamicToken != null) {
                        byte[] tokenBytes = dynamicToken.getBytes(StandardCharsets.UTF_8);
                        out.writeInt(tokenBytes.length);
                        out.write(tokenBytes);
                    }
                    
                    out.writeInt(TeradataBridgeServer.CONTROL_MAGIC);
                    
                    byte[] qidBytes = splitId.getBytes(StandardCharsets.UTF_8);
                    out.writeInt(qidBytes.length);
                    out.write(qidBytes);
                    
                    out.writeInt(3);  // EXPECTED_TERADATA_SIGNALS command
                    out.writeInt(expected);
                    out.flush();
                    log.debug("Broadcast expected connections (%d) to worker %s for query %s", 
                            expected, target, splitId);
                }
            } catch (Exception e) {
                log.warn("Failed to broadcast expected connections to worker %s for query %s: %s", 
                        target, splitId, e.getMessage());
            }
        }
    }

    private void broadcastRegisterToken(List<String> targets, String dynamicToken) {
        if (targets == null || targets.isEmpty() || dynamicToken == null) return;

        for (String target : targets) {
            if (target == null || target.isEmpty()) continue;

            try {
                String[] parts = target.split(":");
                String host = parts[0];
                int port = Integer.parseInt(parts[1]);

                log.debug("Registering dynamic token on worker %s for query %s", target, splitId);
                try (Socket socket = new Socket(host, port);
                     DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {

                    socket.setSoTimeout(config.getBroadcastSocketTimeoutMs());

                    byte[] tokenBytes = dynamicToken.getBytes(StandardCharsets.UTF_8);
                    out.writeInt(tokenBytes.length);
                    out.write(tokenBytes);

                    out.writeInt(TeradataBridgeServer.CONTROL_MAGIC);

                    byte[] qidBytes = splitId.getBytes(StandardCharsets.UTF_8);
                    out.writeInt(qidBytes.length);
                    out.write(qidBytes);

                    out.writeInt(5); // REGISTER_TOKEN command
                    out.flush();
                }
            } catch (Exception e) {
                log.warn("Failed to register dynamic token on worker %s: %s", target, e.getMessage());
            }
        }
    }

    private List<String> parseTargetList(String targetIps) {
        if (targetIps == null || targetIps.isEmpty()) {
            return List.of();
        }

        List<String> targets = new ArrayList<>();
        String[] parts = targetIps.split(",");
        for (String part : parts) {
            String target = part.trim();
            if (!target.isEmpty()) {
                targets.add(target);
            }
        }
        return targets;
    }

    private Connection getConnection() throws SQLException {
        Connection connection = connectionPool.getConnection(session);
        return new PooledConnectionWrapper(connection, connectionPool, trinoUser);
    }

    /**
     * Wrapper that returns connection to pool on close() instead of closing it.
     */
    private static class PooledConnectionWrapper implements Connection {
        private final Connection delegate;
        private final TeradataConnectionPool pool;
        private final String user;
        private boolean closed = false;

        PooledConnectionWrapper(Connection delegate, TeradataConnectionPool pool, String user) {
            this.delegate = delegate;
            this.pool = pool;
            this.user = user;
        }

        @Override
        public void close() throws SQLException {
            if (!closed) {
                closed = true;
                pool.returnConnection(user, delegate);
            }
        }

        @Override public java.sql.Statement createStatement() throws SQLException { return delegate.createStatement(); }
        @Override public java.sql.PreparedStatement prepareStatement(String sql) throws SQLException { return delegate.prepareStatement(sql); }
        @Override public java.sql.CallableStatement prepareCall(String sql) throws SQLException { return delegate.prepareCall(sql); }
        @Override public String nativeSQL(String sql) throws SQLException { return delegate.nativeSQL(sql); }
        @Override public void setAutoCommit(boolean autoCommit) throws SQLException { delegate.setAutoCommit(autoCommit); }
        @Override public boolean getAutoCommit() throws SQLException { return delegate.getAutoCommit(); }
        @Override public void commit() throws SQLException { delegate.commit(); }
        @Override public void rollback() throws SQLException { delegate.rollback(); }
        @Override public boolean isClosed() throws SQLException { return closed || delegate.isClosed(); }
        @Override public java.sql.DatabaseMetaData getMetaData() throws SQLException { return delegate.getMetaData(); }
        @Override public void setReadOnly(boolean readOnly) throws SQLException { delegate.setReadOnly(readOnly); }
        @Override public boolean isReadOnly() throws SQLException { return delegate.isReadOnly(); }
        @Override public void setCatalog(String catalog) throws SQLException { delegate.setCatalog(catalog); }
        @Override public String getCatalog() throws SQLException { return delegate.getCatalog(); }
        @Override public void setTransactionIsolation(int level) throws SQLException { delegate.setTransactionIsolation(level); }
        @Override public int getTransactionIsolation() throws SQLException { return delegate.getTransactionIsolation(); }
        @Override public java.sql.SQLWarning getWarnings() throws SQLException { return delegate.getWarnings(); }
        @Override public void clearWarnings() throws SQLException { delegate.clearWarnings(); }
        @Override public java.sql.Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException { return delegate.createStatement(resultSetType, resultSetConcurrency); }
        @Override public java.sql.PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException { return delegate.prepareStatement(sql, resultSetType, resultSetConcurrency); }
        @Override public java.sql.CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException { return delegate.prepareCall(sql, resultSetType, resultSetConcurrency); }
        @Override public java.util.Map<String, Class<?>> getTypeMap() throws SQLException { return delegate.getTypeMap(); }
        @Override public void setTypeMap(java.util.Map<String, Class<?>> map) throws SQLException { delegate.setTypeMap(map); }
        @Override public void setHoldability(int holdability) throws SQLException { delegate.setHoldability(holdability); }
        @Override public int getHoldability() throws SQLException { return delegate.getHoldability(); }
        @Override public java.sql.Savepoint setSavepoint() throws SQLException { return delegate.setSavepoint(); }
        @Override public java.sql.Savepoint setSavepoint(String name) throws SQLException { return delegate.setSavepoint(name); }
        @Override public void rollback(java.sql.Savepoint savepoint) throws SQLException { delegate.rollback(savepoint); }
        @Override public void releaseSavepoint(java.sql.Savepoint savepoint) throws SQLException { delegate.releaseSavepoint(savepoint); }
        @Override public java.sql.Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException { return delegate.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability); }
        @Override public java.sql.PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException { return delegate.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability); }
        @Override public java.sql.CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException { return delegate.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability); }
        @Override public java.sql.PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException { return delegate.prepareStatement(sql, autoGeneratedKeys); }
        @Override public java.sql.PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException { return delegate.prepareStatement(sql, columnIndexes); }
        @Override public java.sql.PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException { return delegate.prepareStatement(sql, columnNames); }
        @Override public java.sql.Clob createClob() throws SQLException { return delegate.createClob(); }
        @Override public java.sql.Blob createBlob() throws SQLException { return delegate.createBlob(); }
        @Override public java.sql.NClob createNClob() throws SQLException { return delegate.createNClob(); }
        @Override public java.sql.SQLXML createSQLXML() throws SQLException { return delegate.createSQLXML(); }
        @Override public boolean isValid(int timeout) throws SQLException { return delegate.isValid(timeout); }
        @Override public void setClientInfo(String name, String value) throws java.sql.SQLClientInfoException { delegate.setClientInfo(name, value); }
        @Override public void setClientInfo(java.util.Properties properties) throws java.sql.SQLClientInfoException { delegate.setClientInfo(properties); }
        @Override public String getClientInfo(String name) throws SQLException { return delegate.getClientInfo(name); }
        @Override public java.util.Properties getClientInfo() throws SQLException { return delegate.getClientInfo(); }
        @Override public java.sql.Array createArrayOf(String typeName, Object[] elements) throws SQLException { return delegate.createArrayOf(typeName, elements); }
        @Override public java.sql.Struct createStruct(String typeName, Object[] attributes) throws SQLException { return delegate.createStruct(typeName, attributes); }
        @Override public void setSchema(String schema) throws SQLException { delegate.setSchema(schema); }
        @Override public String getSchema() throws SQLException { return delegate.getSchema(); }
        @Override public void abort(java.util.concurrent.Executor executor) throws SQLException { delegate.abort(executor); }
        @Override public void setNetworkTimeout(java.util.concurrent.Executor executor, int milliseconds) throws SQLException { delegate.setNetworkTimeout(executor, milliseconds); }
        @Override public int getNetworkTimeout() throws SQLException { return delegate.getNetworkTimeout(); }
        @Override public <T> T unwrap(Class<T> iface) throws SQLException { return delegate.unwrap(iface); }
        @Override public boolean isWrapperFor(Class<?> iface) throws SQLException { return delegate.isWrapperFor(iface); }
    }

@Override
    public void close() { closed.set(true); }

    @Override
    public boolean isFinished() { return closed.get() || splitsReturned.get(); }
}
