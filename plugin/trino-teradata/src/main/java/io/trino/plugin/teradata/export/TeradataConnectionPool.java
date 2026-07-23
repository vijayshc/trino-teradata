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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.spi.connector.ConnectorSession;

import javax.annotation.PreDestroy;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Per-user JDBC connection pool for Teradata with hard max in-flight limits,
 * blocking acquire under load, idle eviction, and default-user pre-warm.
 */
@Singleton
public class TeradataConnectionPool {
    private static final Logger log = Logger.get(TeradataConnectionPool.class);

    private static final long ACQUIRE_TIMEOUT_MS = 30_000L;

    private final ConcurrentHashMap<String, UserPool> userPools = new ConcurrentHashMap<>();
    /** Maps checked-out Connection → pool metadata for return path. */
    private final ConcurrentHashMap<Connection, PooledConnection> checkedOutContext = new ConcurrentHashMap<>();
    private final TrinoExportConfig config;
    private volatile Driver driver;
    private volatile boolean shutdown = false;

    private static final class PooledConnection {
        final Connection connection;
        volatile long lastUsedMs;
        /** Last PROXYUSER applied; skip redundant QUERY_BAND round-trips. */
        volatile String proxyUser;
        /** Last DATABASE applied. */
        volatile String currentDatabase;

        PooledConnection(Connection connection, String proxyUser, String currentDatabase) {
            this.connection = connection;
            this.lastUsedMs = System.currentTimeMillis();
            this.proxyUser = proxyUser;
            this.currentDatabase = currentDatabase;
        }
    }

    /**
     * total = idle + checked-out (hard cap at maxSize).
     * Checkout prefers idle; creates only when total &lt; maxSize.
     */
    private static final class UserPool {
        final int maxSize;
        final BlockingQueue<PooledConnection> idle;
        final AtomicInteger total = new AtomicInteger(0);

        UserPool(int maxSize) {
            this.maxSize = maxSize;
            this.idle = new LinkedBlockingQueue<>(maxSize);
        }
    }

    @Inject
    public TeradataConnectionPool(TrinoExportConfig config) {
        this.config = config;
        log.info("TeradataConnectionPool initialized: minSize=%d, maxSize=%d, maxIdleMs=%d",
                config.getConnectionPoolMinSize(),
                config.getConnectionPoolMaxSize(),
                config.getConnectionPoolMaxIdleMs());
        prewarmDefaultUser();
    }

    private void prewarmDefaultUser() {
        int min = Math.min(config.getConnectionPoolMinSize(), config.getConnectionPoolMaxSize());
        if (min <= 0) {
            return;
        }
        String userKey = "__default__";
        UserPool pool = getOrCreatePool(userKey);
        int warmed = 0;
        for (int i = 0; i < min; i++) {
            if (pool.total.get() >= pool.maxSize) {
                break;
            }
            if (pool.total.incrementAndGet() > pool.maxSize) {
                pool.total.decrementAndGet();
                break;
            }
            try {
                Connection conn = createNewConnection(null);
                if (!pool.idle.offer(new PooledConnection(conn, null, null))) {
                    closeQuietly(conn);
                    pool.total.decrementAndGet();
                    break;
                }
                warmed++;
            }
            catch (SQLException e) {
                pool.total.decrementAndGet();
                log.warn("Connection pool pre-warm failed: %s", e.getMessage());
                break;
            }
        }
        log.info("Connection pool pre-warmed %d connections for default user", warmed);
    }

    public Connection getConnection(String trinoUser) throws SQLException {
        return getConnection(trinoUser, null);
    }

    public Connection getConnection(ConnectorSession session) throws SQLException {
        return getConnection(session != null ? session.getUser() : null, session);
    }

    private Connection getConnection(String trinoUser, ConnectorSession session) throws SQLException {
        if (shutdown) {
            throw new SQLException("Connection pool is shut down");
        }

        String userKey = trinoUser != null ? trinoUser : "__default__";
        UserPool pool = getOrCreatePool(userKey);
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(ACQUIRE_TIMEOUT_MS);

        while (true) {
            if (shutdown) {
                throw new SQLException("Connection pool is shut down");
            }

            // 1) Prefer idle
            PooledConnection pooled = pollValidIdle(pool);
            if (pooled != null) {
                applySessionContextCached(pooled, trinoUser, session);
                log.debug("Connection pool HIT for user %s", userKey);
                return pooled.connection;
            }

            // 2) Create if under cap
            int current = pool.total.get();
            if (current < pool.maxSize && pool.total.compareAndSet(current, current + 1)) {
                try {
                    // Another thread may have returned while we incremented — prefer idle first
                    pooled = pollValidIdle(pool);
                    if (pooled != null) {
                        // Idle connection already counted in total; drop create reservation
                        pool.total.decrementAndGet();
                        applySessionContextCached(pooled, trinoUser, session);
                        log.debug("Connection pool HIT (after create race) for user %s", userKey);
                        return pooled.connection;
                    }

                    log.debug("Connection pool MISS for user %s, creating new connection", userKey);
                    Connection newConn = createNewConnection(trinoUser);
                    try {
                        String db = applyDatabaseOnly(newConn, session);
                        // Track as checked-out via a transient wrapper stored only for return path
                        // Proxy already applied in createNewConnection
                        checkedOutContext.put(newConn, new PooledConnection(newConn, trinoUser, db));
                    }
                    catch (SQLException e) {
                        closeQuietly(newConn);
                        pool.total.decrementAndGet();
                        throw e;
                    }
                    return newConn;
                }
                catch (SQLException e) {
                    pool.total.decrementAndGet();
                    throw e;
                }
                catch (RuntimeException e) {
                    pool.total.decrementAndGet();
                    throw e;
                }
            }

            // 3) At capacity — wait for a returned connection
            long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
            if (remainingMs <= 0) {
                throw new SQLException("Timed out waiting for Teradata connection (pool exhausted for user " + userKey + ")");
            }
            try {
                pooled = pool.idle.poll(Math.min(remainingMs, 100), TimeUnit.MILLISECONDS);
                if (pooled != null) {
                    if (isUsable(pool, pooled)) {
                        applySessionContextCached(pooled, trinoUser, session);
                        log.debug("Connection pool HIT (after wait) for user %s", userKey);
                        return pooled.connection;
                    }
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SQLException("Interrupted while waiting for pooled connection", e);
            }
        }
    }

    private PooledConnection pollValidIdle(UserPool pool) {
        PooledConnection pooled;
        while ((pooled = pool.idle.poll()) != null) {
            if (isUsable(pool, pooled)) {
                return pooled;
            }
        }
        return null;
    }

    private boolean isUsable(UserPool pool, PooledConnection pooled) {
        long maxIdle = config.getConnectionPoolMaxIdleMs();
        long now = System.currentTimeMillis();
        if (maxIdle > 0 && (now - pooled.lastUsedMs) > maxIdle) {
            discard(pool, pooled.connection);
            return false;
        }
        // Skip isValid() on the hot path when recently used — it is a network round-trip
        // on some drivers and dominates small-query latency under high QPM.
        if ((now - pooled.lastUsedMs) < 30_000L) {
            try {
                return !pooled.connection.isClosed();
            }
            catch (SQLException e) {
                discard(pool, pooled.connection);
                return false;
            }
        }
        try {
            if (pooled.connection.isValid(0)) {
                return true;
            }
        }
        catch (SQLException ignore) {
        }
        discard(pool, pooled.connection);
        return false;
    }

    private void discard(UserPool pool, Connection conn) {
        checkedOutContext.remove(conn);
        closeQuietly(conn);
        pool.total.decrementAndGet();
    }

    private void applySessionContextCached(PooledConnection pooled, String trinoUser, ConnectorSession session)
            throws SQLException {
        String desiredUser = trinoUser;
        if (desiredUser != null && !desiredUser.isBlank()
                && (pooled.proxyUser == null || !pooled.proxyUser.equals(desiredUser))) {
            TeradataSessionContext.applyProxyUser(pooled.connection, desiredUser, config);
            pooled.proxyUser = desiredUser;
        }

        String db = null;
        if (session != null) {
            db = TeradataSessionContext.extractSchema(session).orElse(null);
        }
        if (db != null && !db.isBlank()
                && (pooled.currentDatabase == null || !pooled.currentDatabase.equalsIgnoreCase(db))) {
            TeradataSessionContext.applyCurrentDatabase(pooled.connection, db);
            pooled.currentDatabase = db;
        }
        checkedOutContext.put(pooled.connection, pooled);
    }

    private String applyDatabaseOnly(Connection conn, ConnectorSession session) throws SQLException {
        if (session == null) {
            return null;
        }
        String db = TeradataSessionContext.extractSchema(session).orElse(null);
        if (db != null && !db.isBlank()) {
            TeradataSessionContext.applyCurrentDatabase(conn, db);
        }
        return db;
    }

    public void returnConnection(String trinoUser, Connection conn) {
        if (shutdown || conn == null) {
            if (conn != null) {
                String userKey = trinoUser != null ? trinoUser : "__default__";
                UserPool pool = userPools.get(userKey);
                if (pool != null) {
                    discard(pool, conn);
                }
                else {
                    closeQuietly(conn);
                }
            }
            return;
        }

        String userKey = trinoUser != null ? trinoUser : "__default__";
        UserPool pool = userPools.get(userKey);
        if (pool == null) {
            closeQuietly(conn);
            checkedOutContext.remove(conn);
            return;
        }

        try {
            if (conn.isClosed()) {
                discard(pool, conn);
                return;
            }
        }
        catch (SQLException e) {
            discard(pool, conn);
            return;
        }

        PooledConnection meta = checkedOutContext.remove(conn);
        PooledConnection pooled = meta != null
                ? meta
                : new PooledConnection(conn, trinoUser, null);
        pooled.lastUsedMs = System.currentTimeMillis();
        if (!pool.idle.offer(pooled)) {
            discard(pool, conn);
            log.debug("Pool idle queue full for user %s, closed connection", userKey);
        }
        else {
            log.debug("Returned connection to pool for user %s", userKey);
        }
    }

    private UserPool getOrCreatePool(String userKey) {
        return userPools.computeIfAbsent(userKey, k -> new UserPool(config.getConnectionPoolMaxSize()));
    }

    private Connection createNewConnection(String trinoUser) throws SQLException {
        ensureDriverLoaded();

        Properties props = new Properties();
        props.setProperty("user", config.getTeradataUser());
        props.setProperty("password", config.getTeradataPassword());
        props.setProperty("CHARSET", "UTF8");
        props.setProperty("TMODE", "TERA");

        Connection conn = driver.connect(config.getTeradataUrl(), props);

        if (trinoUser != null && !trinoUser.isEmpty()) {
            String qb = "PROXYUSER=" + trinoUser + ";";
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("SET QUERY_BAND = '" + qb + "' FOR SESSION;");
                log.debug("Set PROXYUSER=%s for new connection", trinoUser);
            }
            catch (SQLException e) {
                if (config.isEnforceProxyAuthentication()) {
                    closeQuietly(conn);
                    throw new SQLException("Proxy authentication failed for user " + trinoUser + ": " + e.getMessage(), e);
                }
                log.warn("Proxy authentication warning for user %s: %s", trinoUser, e.getMessage());
            }
        }

        return conn;
    }

    private void applySessionContext(Connection conn, String trinoUser, ConnectorSession session) throws SQLException {
        if (session != null) {
            TeradataSessionContext.apply(conn, session, config);
            return;
        }
        TeradataSessionContext.applyProxyUser(conn, trinoUser, config);
    }

    private synchronized void ensureDriverLoaded() throws SQLException {
        if (driver == null) {
            try {
                log.info("Loading Teradata JDBC driver...");
                driver = (Driver) Class.forName("com.teradata.jdbc.TeraDriver").getDeclaredConstructor().newInstance();
            }
            catch (Exception e) {
                throw new SQLException("Failed to load Teradata JDBC driver", e);
            }
        }
    }

    private void closeQuietly(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            }
            catch (SQLException ignore) {
            }
        }
    }

    @PreDestroy
    public void shutdown() {
        shutdown = true;
        log.info("Shutting down connection pool...");

        int closedCount = 0;
        for (UserPool pool : userPools.values()) {
            PooledConnection pooled;
            while ((pooled = pool.idle.poll()) != null) {
                closeQuietly(pooled.connection);
                closedCount++;
            }
            pool.total.set(0);
        }
        userPools.clear();

        log.info("Connection pool shutdown complete. Closed %d connections.", closedCount);
    }

    public String getStats() {
        StringBuilder sb = new StringBuilder("ConnectionPool Stats: ");
        for (var entry : userPools.entrySet()) {
            UserPool pool = entry.getValue();
            sb.append(entry.getKey())
                    .append("(total=").append(pool.total.get())
                    .append(",idle=").append(pool.idle.size())
                    .append(",max=").append(pool.maxSize)
                    .append(") ");
        }
        return sb.toString();
    }
}
