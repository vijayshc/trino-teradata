package io.trino.plugin.teradata.export;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.StaticCredentialProvider;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;

import static io.airlift.configuration.ConfigBinder.configBinder;

/**
 * Guice module for Teradata JDBC connector configuration.
 * 
 * Binds:
 * - TeradataClient as the JdbcClient implementation
 * - ConnectionFactory with Teradata driver
 * - Session properties for runtime configuration
 */
public class TeradataClientModule implements Module {
    
    @Override
    public void configure(Binder binder) {
        // Bind TeradataClient as the JDBC client
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(TeradataClient.class).in(Scopes.SINGLETON);
        
        // Bind configuration
        configBinder(binder).bindConfig(BaseJdbcConfig.class);
        configBinder(binder).bindConfig(TrinoExportConfig.class);
        
        // Bind session properties
        binder.bind(TeradataSessionProperties.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory createConnectionFactory(
            BaseJdbcConfig config,
            TrinoExportConfig teradataConfig,
            OpenTelemetry openTelemetry) throws SQLException {
        
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("CHARSET", "UTF8");
        connectionProperties.setProperty("TMODE", "TERA");
        
        // Create credential provider from config
        CredentialProvider credentialProvider = new StaticCredentialProvider(
                Optional.ofNullable(teradataConfig.getTeradataUser()),
                Optional.ofNullable(teradataConfig.getTeradataPassword()));
        
        // Load Teradata JDBC driver
        Driver driver;
        try {
            driver = (Driver) Class.forName("com.teradata.jdbc.TeraDriver").getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new SQLException("Failed to load Teradata JDBC driver", e);
        }
        
        return DriverConnectionFactory.builder(driver, teradataConfig.getTeradataUrl(), credentialProvider)
                .setConnectionProperties(connectionProperties)
                .setOpenTelemetry(openTelemetry)
                .build();
    }
}
