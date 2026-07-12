package io.trino.plugin.teradata.export;

import io.airlift.log.Logger;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.spi.connector.ConnectorSession;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Per-user JDBC connection pool for Teradata.
 * 
 * Key features:
 * - Per-user pools (PROXYUSER is session-level, requires separate connections per user)
 * - Connection reuse (avoids 2+ second JDBC connection overhead per query)
 * - Configurable pool sizes
 * - Automatic connection validation and cleanup
 */
@Singleton
public class TeradataConnectionPool {
    private static final Logger log = Logger.get(TeradataConnectionPool.class);
    
    private final ConcurrentHashMap<String, BlockingQueue<Connection>> userPools = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> poolSizes = new ConcurrentHashMap<>();
    private final TrinoExportConfig config;
    private volatile Driver driver;
    private volatile boolean shutdown = false;
    
    @Inject
    public TeradataConnectionPool(TrinoExportConfig config) {
        this.config = config;
        log.info("TeradataConnectionPool initialized: minSize=%d, maxSize=%d, maxIdleMs=%d",
                config.getConnectionPoolMinSize(),
                config.getConnectionPoolMaxSize(),
                config.getConnectionPoolMaxIdleMs());
    }
    
    /**
     * Get a connection for the specified Trino user.
     * Connections are pooled per-user because PROXYUSER is set at session level.
     */
    public Connection getConnection(String trinoUser) throws SQLException {
        return getConnection(trinoUser, null);
    }

    /**
     * Get a connection for the specified Trino session.
     *
     * The pool remains per-user, but the Teradata session context is re-applied
     * on every borrow so the current database matches the active Trino schema.
     */
    public Connection getConnection(ConnectorSession session) throws SQLException {
        return getConnection(session != null ? session.getUser() : null, session);
    }

    private Connection getConnection(String trinoUser, ConnectorSession session) throws SQLException {
        if (shutdown) {
            throw new SQLException("Connection pool is shut down");
        }
        
        String userKey = trinoUser != null ? trinoUser : "__default__";
        BlockingQueue<Connection> pool = userPools.computeIfAbsent(userKey, 
            k -> new LinkedBlockingQueue<>(config.getConnectionPoolMaxSize()));
        poolSizes.computeIfAbsent(userKey, k -> new AtomicInteger(0));
        
        Connection conn = pool.poll();
        if (conn != null) {
            try {
                // Use 0 timeout for fastest validation (Teradata driver default behavior)
                // This is safe because we're reusing a recently-used connection
                if (conn.isValid(0)) {
                    applySessionContext(conn, trinoUser, session);
                    log.debug("Connection pool HIT for user %s", userKey);
                    return conn;
                } else {
                    log.debug("Pooled connection invalid, closing and creating new");
                    poolSizes.get(userKey).decrementAndGet();
                    closeQuietly(conn);
                }
            } catch (SQLException e) {
                poolSizes.get(userKey).decrementAndGet();
                closeQuietly(conn);
            }
        }
        
        log.debug("Connection pool MISS for user %s, creating new connection", userKey);
        Connection newConn = createNewConnection(trinoUser);
        poolSizes.get(userKey).incrementAndGet();
        try {
            applySessionContext(newConn, trinoUser, session);
        }
        catch (SQLException e) {
            poolSizes.get(userKey).decrementAndGet();
            closeQuietly(newConn);
            throw e;
        }
        return newConn;
    }
    
    /**
     * Return a connection to the pool for reuse.
     */
    public void returnConnection(String trinoUser, Connection conn) {
        if (shutdown || conn == null) {
            closeQuietly(conn);
            return;
        }
        
        String userKey = trinoUser != null ? trinoUser : "__default__";
        BlockingQueue<Connection> pool = userPools.get(userKey);
        
        if (pool == null) {
            closeQuietly(conn);
            return;
        }
        
        try {
            // Quick validation before returning to pool (0 timeout)
            if (!conn.isValid(0)) {
                poolSizes.get(userKey).decrementAndGet();
                closeQuietly(conn);
                return;
            }
        } catch (SQLException e) {
            poolSizes.get(userKey).decrementAndGet();
            closeQuietly(conn);
            return;
        }
        
        if (!pool.offer(conn)) {
            poolSizes.get(userKey).decrementAndGet();
            closeQuietly(conn);
            log.debug("Pool full for user %s, closed connection", userKey);
        } else {
            log.debug("Returned connection to pool for user %s", userKey);
        }
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
            } catch (SQLException e) {
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
            } catch (Exception e) {
                throw new SQLException("Failed to load Teradata JDBC driver", e);
            }
        }
    }
    
    private void closeQuietly(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException ignore) {
            }
        }
    }
    
    /**
     * Shutdown the pool and close all connections.
     */
    public void shutdown() {
        shutdown = true;
        log.info("Shutting down connection pool...");
        
        int closedCount = 0;
        for (BlockingQueue<Connection> pool : userPools.values()) {
            Connection conn;
            while ((conn = pool.poll()) != null) {
                closeQuietly(conn);
                closedCount++;
            }
        }
        userPools.clear();
        poolSizes.clear();
        
        log.info("Connection pool shutdown complete. Closed %d connections.", closedCount);
    }
    
    /**
     * Get current pool statistics for monitoring.
     */
    public String getStats() {
        StringBuilder sb = new StringBuilder("ConnectionPool Stats: ");
        for (var entry : poolSizes.entrySet()) {
            sb.append(entry.getKey()).append("=").append(entry.getValue().get()).append(" ");
        }
        return sb.toString();
    }
}
