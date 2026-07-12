package io.trino.plugin.teradata.export;

import io.airlift.log.Logger;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Unified factory for creating Teradata JDBC connections with security context.
 * Ensures that QUERY_BAND (PROXYUSER) is correctly set for all connections
 * according to the Trino session user.
 */
public class TeradataConnectionFactory {
    private static final Logger log = Logger.get(TeradataConnectionFactory.class);
    private static Driver driver;

    private TeradataConnectionFactory() {}

    /**
     * Create a new Teradata JDBC connection and initialize the session security context.
     * 
     * @param config The connector configuration
     * @param trinoUser The name of the Trino user to use as PROXYUSER for authorization
     * @return An initialized JDBC Connection
     * @throws Exception if connection fails
     */
    public static synchronized Connection getConnection(TrinoExportConfig config, String trinoUser) throws Exception {
        if (driver == null) {
            log.info("Loading Teradata JDBC driver...");
            driver = (Driver) Class.forName("com.teradata.jdbc.TeraDriver").getDeclaredConstructor().newInstance();
        }

        Properties props = new Properties();
        props.setProperty("user", config.getTeradataUser());
        props.setProperty("password", config.getTeradataPassword());

        // Note: Some Teradata JDBC versions might support QUERY_BAND as a property,
        // but executing it as a SQL statement is universally supported across versions.
        Connection conn = driver.connect(config.getTeradataUrl(), props);

        try {
            TeradataSessionContext.applyProxyUser(conn, trinoUser, config);
            if (trinoUser != null && !trinoUser.isEmpty()) {
                log.info("Successfully established Teradata session with PROXYUSER=%s", trinoUser);
            }
        }
        catch (SQLException e) {
            log.error("Failed to set Teradata QUERY_BAND for user %s: %s. Access denied.", trinoUser, e.getMessage());
            try { conn.close(); } catch (SQLException ignore) {}
            throw e;
        }

        return conn;
    }
}
