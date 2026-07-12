package io.trino.plugin.teradata.export;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.ConfigBinder;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.base.mapping.DefaultIdentifierMapping;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.*;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.StaticCredentialProvider;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.plugin.teradata.export.ptf.TeradataSystemQuery;
import io.trino.spi.function.table.ConnectorTableFunction;

import java.sql.Driver;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

/**
 * Guice module for the Teradata Export Connector.
 */
public class TrinoExportModule implements Module {
    @Override
    public void configure(Binder binder) {
        // Configuration
        ConfigBinder.configBinder(binder).bindConfig(TrinoExportConfig.class);
        ConfigBinder.configBinder(binder).bindConfig(JdbcMetadataConfig.class);
        ConfigBinder.configBinder(binder).bindConfig(TypeHandlingJdbcConfig.class);
        ConfigBinder.configBinder(binder).bindConfig(JdbcWriteConfig.class);
        
        // Core connector components
        binder.bind(TrinoExportConnector.class).in(Scopes.SINGLETON);
        binder.bind(TrinoExportSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(TrinoExportMetadata.class).in(Scopes.SINGLETON);
        binder.bind(TrinoExportFlightServer.class).in(Scopes.SINGLETON);
        binder.bind(TeradataBridgeServer.class).in(Scopes.SINGLETON);
        
        // TeradataClient for JDBC type mappings and expression rewriting
        binder.bind(TeradataClient.class).in(Scopes.SINGLETON);
        binder.bind(JdbcClient.class).to(TeradataClient.class).in(Scopes.SINGLETON);
        
        // TeradataQueryBuilder for SQL generation (extends DefaultQueryBuilder)
        binder.bind(TeradataQueryBuilder.class).in(Scopes.SINGLETON);
        binder.bind(QueryBuilder.class).to(TeradataQueryBuilder.class).in(Scopes.SINGLETON);
        
        // Bind IdentifierMapping for JDBC client
        binder.bind(IdentifierMapping.class).to(DefaultIdentifierMapping.class).in(Scopes.SINGLETON);

        // Bindings for DefaultJdbcMetadata
        binder.bind(TimestampTimeZoneDomain.class).toInstance(TimestampTimeZoneDomain.ANY);
        Multibinder.newSetBinder(binder, JdbcQueryEventListener.class);
        Multibinder.newSetBinder(binder, ConnectorTableFunction.class)
            .addBinding()
            .toProvider(TeradataSystemQuery.class)
            .in(Scopes.SINGLETON);
        
        // Session properties (custom implementation)
        binder.bind(TrinoExportSessionProperties.class).in(Scopes.SINGLETON);

        // Connection pool for JDBC connections (per-user pooling)
        binder.bind(TeradataConnectionPool.class).in(Scopes.SINGLETON);

        // Background metadata refresh service (eager binding to start on plugin load)
        binder.bind(MetadataRefreshService.class).asEagerSingleton();
    }

    @Provides
    @Singleton
    public BaseJdbcConfig provideBaseJdbcConfig(TrinoExportConfig config) {
        BaseJdbcConfig jdbcConfig = new BaseJdbcConfig();
        jdbcConfig.setConnectionUrl(config.getTeradataUrl());
        return jdbcConfig;
    }

    @Provides
    @Singleton
    public RemoteQueryModifier provideRemoteQueryModifier() {
        return RemoteQueryModifier.NONE;
    }

    @Provides
    @Singleton
    public ConnectionFactory provideConnectionFactory(TrinoExportConfig config, OpenTelemetry openTelemetry) throws SQLException {
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("CHARSET", "UTF8");
        connectionProperties.setProperty("TMODE", "TERA");
        
        CredentialProvider credentialProvider = new StaticCredentialProvider(
                Optional.ofNullable(config.getTeradataUser()),
                Optional.ofNullable(config.getTeradataPassword()));
        
        Driver driver;
        try {
            driver = (Driver) Class.forName("com.teradata.jdbc.TeraDriver").getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new SQLException("Failed to load Teradata JDBC driver", e);
        }
        
        ConnectionFactory delegate = DriverConnectionFactory.builder(driver, config.getTeradataUrl(), credentialProvider)
                .setConnectionProperties(connectionProperties)
                .setOpenTelemetry(openTelemetry)
                .build();

        return new ConnectionFactory() {
            @Override
            public java.sql.Connection openConnection(io.trino.spi.connector.ConnectorSession session) throws java.sql.SQLException {
                java.sql.Connection connection = delegate.openConnection(session);
                try {
                    TeradataSessionContext.apply(connection, session, config);
                    return connection;
                }
                catch (java.sql.SQLException | RuntimeException e) {
                    try {
                        connection.close();
                    }
                    catch (java.sql.SQLException closeException) {
                        e.addSuppressed(closeException);
                    }
                    throw e;
                }
            }

            @Override
            public void close() throws java.sql.SQLException {
                delegate.close();
            }
        };
    }

    @Provides
    @Singleton
    public OpenTelemetry provideOpenTelemetry() {
        return OpenTelemetry.noop();
    }
}
