package io.trino.plugin.teradata.export;

import io.airlift.log.Logger;
import io.trino.spi.connector.ConnectorSession;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Applies Trino session context to Teradata JDBC connections.
 *
 * This is required for prepared-query analysis used by {@code system.query}, because
 * those JDBC sessions must see the same PROXYUSER authorization and current database
 * that normal connector execution uses.
 */
public final class TeradataSessionContext {
    private static final Logger log = Logger.get(TeradataSessionContext.class);

    private TeradataSessionContext() {}

    public static void apply(Connection connection, ConnectorSession session, TrinoExportConfig config)
            throws SQLException {
        requireNonNull(connection, "connection is null");
        requireNonNull(config, "config is null");

        if (session == null) {
            return;
        }

        applyProxyUser(connection, session.getUser(), config);
        Optional<String> schema = extractSchema(session);
        if (schema.isPresent()) {
            applyCurrentDatabase(connection, schema.get());
        }
    }

    public static void applyProxyUser(Connection connection, String trinoUser, TrinoExportConfig config)
            throws SQLException {
        if (trinoUser == null || trinoUser.isBlank()) {
            return;
        }

        String queryBand = "PROXYUSER=" + trinoUser + ";";
        try (Statement statement = connection.createStatement()) {
            statement.execute("SET QUERY_BAND = '" + queryBand + "' FOR SESSION;");
        }
        catch (SQLException e) {
            if (config.isEnforceProxyAuthentication()) {
                throw new SQLException("Proxy authentication failed for user " + trinoUser + ": " + e.getMessage(), e);
            }
            log.warn("Proxy authentication warning for user %s: %s", trinoUser, e.getMessage());
        }
    }

    public static Optional<String> extractSchema(ConnectorSession session) {
        if (session == null) {
            return Optional.empty();
        }

        // Public ConnectorSession SPI does not expose schema in Trino 479, but the runtime
        // FullConnectorSession wraps io.trino.Session, which does. Use reflection so the
        // connector stays compiled only against trino-spi.
        try {
            Method getSessionMethod = session.getClass().getMethod("getSession");
            Object trinoSession = getSessionMethod.invoke(session);
            if (trinoSession == null) {
                return Optional.empty();
            }

            Method getSchemaMethod = trinoSession.getClass().getMethod("getSchema");
            Object value = getSchemaMethod.invoke(trinoSession);
            if (value instanceof Optional<?> optional && optional.isPresent()) {
                Object schema = optional.get();
                if (schema instanceof String schemaName && !schemaName.isBlank()) {
                    return Optional.of(schemaName);
                }
            }
        }
        catch (ReflectiveOperationException e) {
            log.debug(e, "Unable to extract Trino session schema reflectively from %s", session.getClass().getName());
        }

        return Optional.empty();
    }

    public static void applyCurrentDatabase(Connection connection, String schema)
            throws SQLException {
        if (schema == null || schema.isBlank()) {
            return;
        }

        String normalizedSchema = schema.trim();
        if (!isSafeUnquotedIdentifier(normalizedSchema)) {
            log.warn("Skipping Teradata DATABASE switch for schema '%s' because it is not a safe unquoted identifier", normalizedSchema);
            return;
        }

        try (Statement statement = connection.createStatement()) {
            statement.execute("DATABASE " + normalizedSchema);
        }
    }

    private static boolean isSafeUnquotedIdentifier(String value) {
        return value.matches("[A-Za-z_][A-Za-z0-9_#$]*");
    }
}