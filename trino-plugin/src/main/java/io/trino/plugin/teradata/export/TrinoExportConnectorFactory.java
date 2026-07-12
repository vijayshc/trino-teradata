package io.trino.plugin.teradata.export;

import io.airlift.bootstrap.Bootstrap;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class TrinoExportConnectorFactory implements ConnectorFactory {
    @Override
    public String getName() {
        return "teradata_export";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
        System.out.println("DEBUG: TrinoExportConnectorFactory.create called for catalog: " + catalogName);
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(config, "config is null");

        try {
            Bootstrap app = new Bootstrap(
                    new TrinoExportModule(),
                    binder -> {
                        binder.bind(io.trino.spi.NodeManager.class).toInstance(context.getNodeManager());
                    });

            return app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize()
                    .getInstance(TrinoExportConnector.class);
        } catch (Exception e) {
            System.err.println("DEBUG ERROR in factory: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
