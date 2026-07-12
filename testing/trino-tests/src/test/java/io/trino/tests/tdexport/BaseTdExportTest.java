package io.trino.tests.tdexport;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class BaseTdExportTest {
    protected static final Logger log = LoggerFactory.getLogger(BaseTdExportTest.class);
    protected static Connection connection;
    protected static LocalDateTime sessionStartTime;

    /**
     * Connection settings are resolved from (highest precedence first):
     * 1) JVM system property (-Dtrino.jdbc.url=...)
     * 2) Environment variable (TRINO_JDBC_URL, TRINO_USER, TRINO_SERVER_LOG)
     * 3) Built-in defaults suitable for a local multi-node lab
     */
    protected static final String TRINO_URL = firstNonBlank(
            System.getProperty("trino.jdbc.url"),
            System.getenv("TRINO_JDBC_URL"),
            "jdbc:trino://localhost:8080/tdexport/trinoexport");
    protected static final String TRINO_USER = firstNonBlank(
            System.getProperty("trino.user"),
            System.getenv("TRINO_USER"),
            "vijay");
    protected static final String LOG_PATH = firstNonBlank(
            System.getProperty("trino.server.log"),
            System.getenv("TRINO_SERVER_LOG"),
            defaultServerLog());
    protected static final DateTimeFormatter LOG_TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
    private static final long TRINO_STARTUP_TIMEOUT_MS = 180_000;
    private static final long TRINO_STARTUP_RETRY_DELAY_MS = 2_000;

    private static String firstNonBlank(String... values) {
        for (String v : values) {
            if (v != null && !v.isBlank()) {
                return v;
            }
        }
        return "";
    }

    private static String defaultServerLog() {
        // Prefer the well-known local lab path when present; otherwise empty (log tests skip/fail clearly).
        String lab = System.getProperty("user.home") + "/tdconnector/trino_server/trino-server-479/data/var/log/server.log";
        if (java.nio.file.Files.isRegularFile(java.nio.file.Paths.get(lab))) {
            return lab;
        }
        return lab;
    }

    @BeforeAll
    public static void setup() throws SQLException {
        sessionStartTime = LocalDateTime.now().minusMinutes(30); // Include logs from 30 minutes before session
        log.info("Connecting to Trino at {} as {}", TRINO_URL, TRINO_USER);
        log.info("Server log path: {}", LOG_PATH);
        waitForTrinoReady();
        connection = DriverManager.getConnection(TRINO_URL, TRINO_USER, null);
    }

    @AfterAll
    public static void teardown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @BeforeEach
    public void logTestStart(TestInfo testInfo) {
        log.info("Running: {}", testInfo.getDisplayName());
    }

    protected void assertQuery(String sql, Object expected) {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            assertThat(rs.next()).as("No data returned for query: " + sql).isTrue();
            Object actual = rs.getObject(1);

            String actualStr = actual != null ? actual.toString().trim() : "";
            String expectedStr = expected != null ? expected.toString().trim() : "";

            assertThat(actualStr).as("Mismatch for query: " + sql).isEqualTo(expectedStr);
        } catch (SQLException e) {
            throw new RuntimeException("Query failed: " + sql, e);
        }
    }

    protected void assertQueryContains(String sql, String expectedSubstring) {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            assertThat(rs.next()).isTrue();
            String actual = rs.getString(1);
            assertThat(actual).as("Query result does not contain: " + expectedSubstring)
                    .contains(expectedSubstring);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected List<String> getQueryResult(String sql) {
        List<String> results = new ArrayList<>();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                results.add(rs.getString(1));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return results;
    }

    protected List<String> getColumnNames(String sql) {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            ResultSetMetaData metaData = rs.getMetaData();
            List<String> columns = new ArrayList<>();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                columns.add(metaData.getColumnLabel(i));
            }
            return columns;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to read column names for query: " + sql, e);
        }
    }

    protected void executeStatement(String sql) {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException("Statement failed: " + sql, e);
        }
    }

    protected void assertQueryFails(String sql, String expectedMessagePart) {
        assertThatThrownBy(() -> {
            try (Statement stmt = connection.createStatement()) {
                stmt.executeQuery(sql);
            }
        })
                .isInstanceOf(SQLException.class)
                .hasMessageContaining(expectedMessagePart);
    }

    protected void assertLogContains(String... expectedSubstrings) {
        try {
            Thread.sleep(1500);

            List<String> lines = java.nio.file.Files.readAllLines(java.nio.file.Paths.get(LOG_PATH));

            for (int i = lines.size() - 1; i >= 0; i--) {
                String line = lines.get(i);

                // Check if line is within session time window
                if (!isLineInSessionWindow(line)) {
                    continue;
                }

                String lineLower = line.toLowerCase();
                boolean allFound = true;
                for (String expected : expectedSubstrings) {
                    if (!lineLower.contains(expected.toLowerCase())) {
                        allFound = false;
                        break;
                    }
                }
                if (allFound) return;
            }

            StringBuilder debugInfo = new StringBuilder();
            debugInfo.append("Log validation failed. Expected substrings: ").append(java.util.Arrays.toString(expectedSubstrings)).append("\n");
            debugInfo.append("Session start: ").append(sessionStartTime).append("\n");
            debugInfo.append("Last 10 lines of log:\n");
            int debugStart = Math.max(0, lines.size() - 10);
            for (int i = debugStart; i < lines.size(); i++) {
                debugInfo.append(lines.get(i)).append("\n");
            }

            assertThat(false).as(debugInfo.toString()).isTrue();
        } catch (Exception e) {
            throw new RuntimeException("Failed to validate logs", e);
        }
    }

    protected boolean checkLogFor(String... expectedSubstrings) {
        try {
            List<String> lines = java.nio.file.Files.readAllLines(java.nio.file.Paths.get(LOG_PATH));

            for (int i = lines.size() - 1; i >= 0; i--) {
                String line = lines.get(i);

                // Check if line is within session time window
                if (!isLineInSessionWindow(line)) {
                    continue;
                }

                String lineLower = line.toLowerCase();
                boolean allFound = true;
                for (String expected : expectedSubstrings) {
                    if (!lineLower.contains(expected.toLowerCase())) {
                        allFound = false;
                        break;
                    }
                }
                if (allFound) return true;
            }
            return false;
        } catch (IOException e) {
            return false;
        }
    }

    private boolean isLineInSessionWindow(String line) {
        try {
            // Log format: 2025-12-31T12:55:50.695+0800
            if (line.length() < 19) return false;
            String timestampStr = line.substring(0, 19);
            LocalDateTime lineTime = LocalDateTime.parse(timestampStr, LOG_TIMESTAMP_FORMATTER);
            return !lineTime.isBefore(sessionStartTime);
        } catch (Exception e) {
            return true; // Include lines that don't match timestamp format
        }
    }

    private static void waitForTrinoReady() throws SQLException {
        long deadline = System.currentTimeMillis() + TRINO_STARTUP_TIMEOUT_MS;
        SQLException lastException = null;

        while (System.currentTimeMillis() < deadline) {
            try (Connection probeConnection = DriverManager.getConnection(TRINO_URL, TRINO_USER, null);
                 Statement stmt = probeConnection.createStatement();
                 ResultSet rs = stmt.executeQuery("SHOW TABLES")) {
                return;
            } catch (SQLException e) {
                lastException = e;
                String message = e.getMessage() == null ? "" : e.getMessage().toLowerCase();
                if (!(message.contains("still initializing")
                        || message.contains("connection refused")
                        || message.contains("connect timed out")
                        || message.contains("service unavailable"))) {
                    throw e;
                }

                try {
                    Thread.sleep(TRINO_STARTUP_RETRY_DELAY_MS);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                    SQLException interrupted = new SQLException("Interrupted while waiting for Trino readiness", interruptedException);
                    if (lastException != null) {
                        interrupted.addSuppressed(lastException);
                    }
                    throw interrupted;
                }
            }
        }

        throw new SQLException("Timed out waiting for Trino to become ready", lastException);
    }
}
