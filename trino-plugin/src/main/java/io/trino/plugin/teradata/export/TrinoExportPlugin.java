package io.trino.plugin.teradata.export;

public class TrinoExportPlugin implements io.trino.spi.Plugin {
    @Override
    public java.lang.Iterable<io.trino.spi.connector.ConnectorFactory> getConnectorFactories() {
        return com.google.common.collect.ImmutableList.of(new TrinoExportConnectorFactory());
    }
}
