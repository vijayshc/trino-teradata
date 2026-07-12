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
