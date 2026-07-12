package io.trino.plugin.teradata.export.ptf;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorTableFunction;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.ptf.Query.QueryFunctionHandle;
import io.trino.plugin.teradata.export.TrinoExportConfig;
import io.trino.plugin.teradata.export.TrinoExportMetadata;
import io.trino.plugin.teradata.export.TrinoExportSessionProperties;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

/**
 * Connector-level implementation of system.query for the custom Teradata connector.
 */
public class TeradataSystemQuery implements Provider<ConnectorTableFunction> {
    private final TrinoExportMetadata metadata;
    private final TrinoExportConfig config;

    @Inject
    public TeradataSystemQuery(TrinoExportMetadata metadata, TrinoExportConfig config) {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.config = requireNonNull(config, "config is null");
    }

    @Override
    public ConnectorTableFunction get() {
        return new ClassLoaderSafeConnectorTableFunction(new SystemQueryFunction(metadata, config), getClass().getClassLoader());
    }

    public static class SystemQueryFunction extends AbstractConnectorTableFunction {
        private static final Logger log = Logger.get(SystemQueryFunction.class);
        public static final String SCHEMA_NAME = "system";
        public static final String FUNCTION_NAME = "query";
        private static final String ARGUMENT_NAME = "QUERY";

        private final TrinoExportMetadata metadata;
        private final TrinoExportConfig config;

        public SystemQueryFunction(TrinoExportMetadata metadata, TrinoExportConfig config) {
            super(
                    SCHEMA_NAME,
                    FUNCTION_NAME,
                    List.of(ScalarArgumentSpecification.builder()
                            .name(ARGUMENT_NAME)
                            .type(VARCHAR)
                            .build()),
                    GENERIC_TABLE);
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.config = requireNonNull(config, "config is null");
        }

        @Override
        public TableFunctionAnalysis analyze(
                ConnectorSession session,
                ConnectorTransactionHandle transaction,
                Map<String, Argument> arguments,
                ConnectorAccessControl accessControl) {
            if (!TrinoExportSessionProperties.isSystemQueryEnabled(session)) {
                throw new TrinoException(NOT_SUPPORTED, "system.query is disabled for this session");
            }

            ScalarArgument argument = (ScalarArgument) getOnlyElement(arguments.values());
            Object rawValue = argument.getValue();
            if (!(rawValue instanceof Slice slice)) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "system.query requires a VARCHAR query argument");
            }

            String query = normalizeQuery(slice.toStringUtf8());
            validateQuery(query);

            PreparedQuery preparedQuery = new PreparedQuery(query, ImmutableList.of());
            JdbcTableHandle tableHandle = metadata.getTableHandle(session, preparedQuery);
            if (tableHandle == null) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "system.query could not analyze the provided SQL");
            }

            List<JdbcColumnHandle> columns = tableHandle.getColumns()
                    .orElseThrow(() -> new TrinoException(INVALID_FUNCTION_ARGUMENT,
                            "system.query failed to infer output columns for the provided SQL"));
            if (columns.isEmpty()) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT,
                        "system.query requires the remote query to return at least one column");
            }

            Descriptor returnedType = new Descriptor(columns.stream()
                    .map(column -> new Descriptor.Field(column.getColumnName(), Optional.of(column.getColumnType())))
                    .collect(toImmutableList()));

            log.info("system.query analyzed successfully for query %s (sqlLength=%d, columns=%d)",
                    session.getQueryId(), query.length(), columns.size());

            return TableFunctionAnalysis.builder()
                    .returnedType(returnedType)
                    .handle(new QueryFunctionHandle(tableHandle))
                    .build();
        }

        private String normalizeQuery(String query) {
            String normalized = query == null ? "" : query.trim();
            while (normalized.endsWith(";")) {
                normalized = normalized.substring(0, normalized.length() - 1).trim();
            }
            return normalized;
        }

        private void validateQuery(String query) {
            if (query.isEmpty()) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "system.query requires a non-empty SQL string");
            }

            int maxLength = config.getSystemQueryMaxLength();
            if (maxLength > 0 && query.length() > maxLength) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT,
                        "system.query SQL length exceeds configured maximum of " + maxLength + " characters");
            }
        }
    }
}