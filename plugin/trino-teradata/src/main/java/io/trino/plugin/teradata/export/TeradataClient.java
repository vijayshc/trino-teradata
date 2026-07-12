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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.plugin.base.aggregation.AggregateFunctionRewriter;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongReadFunction;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectReadFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.SliceWriteFunction;
import io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties;
import io.trino.plugin.jdbc.UnsupportedTypeHandling;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.aggregation.ImplementAvgDecimal;
import io.trino.plugin.jdbc.aggregation.ImplementAvgFloatingPoint;
import io.trino.plugin.jdbc.aggregation.ImplementCount;
import io.trino.plugin.jdbc.aggregation.ImplementCountAll;
import io.trino.plugin.jdbc.aggregation.ImplementCountDistinct;
import io.trino.plugin.jdbc.aggregation.ImplementMinMax;
import io.trino.plugin.jdbc.aggregation.ImplementSum;
import io.trino.plugin.jdbc.expression.JdbcConnectorExpressionRewriterBuilder;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.plugin.jdbc.JdbcSortItem;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.function.BiFunction;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.PredicatePushdownController.FULL_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Teradata JDBC Client extending BaseJdbcClient for enterprise-grade SQL construction.
 * 
 * Provides:
 * - Full expression rewriting for predicates (LIKE, OR, CAST, etc.)
 * - Comprehensive type mappings for Teradata data types
 * - Aggregate function pushdown (COUNT, SUM, MIN, MAX, AVG)
 * - Teradata-specific SQL dialect handling
 */
public class TeradataClient extends BaseJdbcClient {
    private static final Logger log = Logger.get(TeradataClient.class);
    
    private static final int TERADATA_VARCHAR_MAX_LENGTH = 64000;
    private static final int TERADATA_CHAR_MAX_LENGTH = 64000;
    private static final int MAX_DECIMAL_PRECISION = 38;

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    private static final Map<Type, WriteMapping> WRITE_MAPPINGS = ImmutableMap.<Type, WriteMapping>builder()
            .put(BIGINT, WriteMapping.longMapping("BIGINT", bigintWriteFunction()))
            .put(INTEGER, WriteMapping.longMapping("INTEGER", integerWriteFunction()))
            .put(SMALLINT, WriteMapping.longMapping("SMALLINT", smallintWriteFunction()))
            .put(TINYINT, WriteMapping.longMapping("BYTEINT", tinyintWriteFunction()))
            .put(DOUBLE, WriteMapping.doubleMapping("FLOAT", doubleWriteFunction()))
            .put(REAL, WriteMapping.longMapping("REAL", realWriteFunction()))
            .put(DATE, WriteMapping.longMapping("DATE", teradataDateWriteFunction()))
            .buildOrThrow();

    private final ConnectorExpressionRewriter<ParameterizedExpression> connectorExpressionRewriter;
    private final AggregateFunctionRewriter<JdbcExpression, ?> aggregateFunctionRewriter;
    private final int metadataCacheSize;

    @Inject
    public TeradataClient(
            BaseJdbcConfig config,
            TrinoExportConfig exportConfig,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier) {
        super("\"", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, queryModifier, true);
        this.metadataCacheSize = exportConfig.getMetadataCacheSize();

        // Build expression rewriter with Teradata SQL rules
        this.connectorExpressionRewriter = JdbcConnectorExpressionRewriterBuilder.newBuilder()
                .addStandardRules(this::quoted)
                // Numeric type comparisons
                .withTypeClass("numeric_type", ImmutableSet.of("tinyint", "smallint", "integer", "bigint", "decimal", "real", "double"))
                .map("$equal(left: numeric_type, right: numeric_type)").to("left = right")
                .map("$not_equal(left: numeric_type, right: numeric_type)").to("left <> right")
                .map("$less_than(left: numeric_type, right: numeric_type)").to("left < right")
                .map("$less_than_or_equal(left: numeric_type, right: numeric_type)").to("left <= right")
                .map("$greater_than(left: numeric_type, right: numeric_type)").to("left > right")
                .map("$greater_than_or_equal(left: numeric_type, right: numeric_type)").to("left >= right")
                // String type comparisons (case-sensitive in Teradata)
                .withTypeClass("string_type", ImmutableSet.of("varchar", "char"))
                .map("$equal(left: string_type, right: string_type)").to("left = right")
                .map("$not_equal(left: string_type, right: string_type)").to("left <> right")
                .map("$less_than(left: string_type, right: string_type)").to("left < right")
                .map("$less_than_or_equal(left: string_type, right: string_type)").to("left <= right")
                .map("$greater_than(left: string_type, right: string_type)").to("left > right")
                .map("$greater_than_or_equal(left: string_type, right: string_type)").to("left >= right")
                // LIKE pattern matching
                .map("$like(value: string_type, pattern: string_type)").to("value LIKE pattern")
                .map("$like(value: string_type, pattern: string_type, escape: string_type)")
                    .to("value LIKE pattern ESCAPE escape")
                // NULL handling
                .map("$is_null(value)").to("value IS NULL")
                .map("$not($is_null(value))").to("value IS NOT NULL")
                // Date/time comparisons
                .withTypeClass("datetime_type", ImmutableSet.of("date", "time", "timestamp"))
                .map("$equal(left: datetime_type, right: datetime_type)").to("left = right")
                .map("$not_equal(left: datetime_type, right: datetime_type)").to("left <> right")
                .map("$less_than(left: datetime_type, right: datetime_type)").to("left < right")
                .map("$less_than_or_equal(left: datetime_type, right: datetime_type)").to("left <= right")
                .map("$greater_than(left: datetime_type, right: datetime_type)").to("left > right")
                .map("$greater_than_or_equal(left: datetime_type, right: datetime_type)").to("left >= right")
                // Arithmetic
                .map("$add(left: numeric_type, right: numeric_type)").to("left + right")
                .map("$subtract(left: numeric_type, right: numeric_type)").to("left - right")
                .map("$multiply(left: numeric_type, right: numeric_type)").to("left * right")
                .map("$divide(left: numeric_type, right: numeric_type)").to("left / right")
                .map("$modulus(left: numeric_type, right: numeric_type)").to("left % right")
                .map("$negate(value: numeric_type)").to("-value")
                // Operator names might be used directly
                .map("$operator$add(left: numeric_type, right: numeric_type)").to("left + right")
                .map("$operator$subtract(left: numeric_type, right: numeric_type)").to("left - right")
                .map("$operator$multiply(left: numeric_type, right: numeric_type)").to("left * right")
                .map("$operator$divide(left: numeric_type, right: numeric_type)").to("left / right")
                .map("$operator$modulus(left: numeric_type, right: numeric_type)").to("left % right")
                .map("$operator$negate(value: numeric_type)").to("-value")
                // Cast (simple pass-through for now, assuming Teradata implicit cast works or is unnecessary for simple int->bigint)
                // Note: Standard rules don't easily support generic CAST target extraction in string pattern.
                .build();

        // Aggregate function rewriter
        JdbcTypeHandle bigintTypeHandle = new JdbcTypeHandle(Types.BIGINT, Optional.of("BIGINT"), Optional.of(19), Optional.of(0), Optional.empty(), Optional.empty());
        this.aggregateFunctionRewriter = new AggregateFunctionRewriter<>(
                connectorExpressionRewriter,
                ImmutableSet.<AggregateFunctionRule<JdbcExpression, ParameterizedExpression>>builder()
                        .add(new ImplementCountAll(bigintTypeHandle))
                        .add(new ImplementCount(bigintTypeHandle))
                        .add(new ImplementCountDistinct(bigintTypeHandle, false))
                        .add(new ImplementMinMax(true))
                        .add(new ImplementSum(TeradataClient::toTypeHandle))
                        .add(new ImplementAvgFloatingPoint())
                        .add(new ImplementAvgDecimal())
                        .build());

        // Initialize metadata cache with configured size
        this.metadataCache = createMetadataCache();

        log.info("TeradataClient initialized with expression rewriter, aggregate support, and cache size %d", metadataCacheSize);
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle) {
        String jdbcTypeName = typeHandle.jdbcTypeName()
                .orElse("");

        switch (typeHandle.jdbcType()) {
            case Types.TINYINT:
                return Optional.of(tinyintColumnMapping());
            case Types.SMALLINT:
                return Optional.of(smallintColumnMapping());
            case Types.INTEGER:
                return Optional.of(integerColumnMapping());
            case Types.BIGINT:
                return Optional.of(bigintColumnMapping());

            case Types.REAL:
            case Types.FLOAT:
            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());

            case Types.DECIMAL:
            case Types.NUMERIC:
                int precision = typeHandle.requiredColumnSize();
                int scale = typeHandle.requiredDecimalDigits();
                if (precision > MAX_DECIMAL_PRECISION) {
                    precision = MAX_DECIMAL_PRECISION;
                }
                if (precision <= 0) {
                    precision = MAX_DECIMAL_PRECISION;
                    scale = max(scale, 0);
                }
                DecimalType decimalType = createDecimalType(precision, scale);
                if (decimalType.isShort()) {
                    return Optional.of(ColumnMapping.longMapping(
                            decimalType,
                            shortDecimalReadFunction(decimalType),
                            shortDecimalWriteFunction(decimalType),
                            FULL_PUSHDOWN));
                }
                return Optional.of(ColumnMapping.objectMapping(
                        decimalType,
                        longDecimalReadFunction(decimalType),
                        longDecimalWriteFunction(decimalType),
                        FULL_PUSHDOWN));

            case Types.CHAR:
                int charLength = min(typeHandle.requiredColumnSize(), TERADATA_CHAR_MAX_LENGTH);
                CharType charType = CharType.createCharType(charLength);
                return Optional.of(ColumnMapping.sliceMapping(
                        charType,
                        charReadFunction(charType),
                        teradataCharWriteFunction(),
                        FULL_PUSHDOWN));

            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                int varcharLength = typeHandle.requiredColumnSize();
                if (varcharLength <= 0 || varcharLength > TERADATA_VARCHAR_MAX_LENGTH) {
                    return Optional.of(ColumnMapping.sliceMapping(
                            createUnboundedVarcharType(),
                            (resultSet, columnIndex) -> utf8Slice(resultSet.getString(columnIndex)),
                            varcharWriteFunction(),
                            DISABLE_PUSHDOWN));
                }
                return Optional.of(ColumnMapping.sliceMapping(
                        createVarcharType(varcharLength),
                        (resultSet, columnIndex) -> utf8Slice(resultSet.getString(columnIndex)),
                        varcharWriteFunction(),
                        FULL_PUSHDOWN));

            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.NCLOB:
            case Types.SQLXML:
            case Types.TIME_WITH_TIMEZONE:
            case Types.TIMESTAMP_WITH_TIMEZONE:
            case Types.DISTINCT:
            case Types.STRUCT:
            case Types.ARRAY:
            case Types.REF:
            case Types.ROWID:
            case Types.DATALINK:
                return mapUnsupportedTypeToVarchar(typeHandle, jdbcTypeName);

            case Types.CLOB:
                return Optional.of(ColumnMapping.sliceMapping(
                        createUnboundedVarcharType(),
                        (resultSet, columnIndex) -> utf8Slice(resultSet.getString(columnIndex)),
                        varcharWriteFunction(),
                        DISABLE_PUSHDOWN));

            case Types.BLOB:
                return Optional.of(varbinaryColumnMapping());

            case Types.DATE:
                return Optional.of(ColumnMapping.longMapping(
                        DATE,
                        teradataDateReadFunction(),
                        teradataDateWriteFunction(),
                        FULL_PUSHDOWN));

            case Types.TIMESTAMP:
                return Optional.of(ColumnMapping.longMapping(
                        TIMESTAMP_MICROS,
                        teradataTimestampReadFunction(),
                        teradataTimestampWriteFunction(),
                        FULL_PUSHDOWN));

            case Types.TIME:
                return Optional.of(ColumnMapping.longMapping(
                        io.trino.spi.type.TimeType.TIME_MICROS,
                        teradataTimeReadFunction(),
                        teradataTimeWriteFunction(),
                        FULL_PUSHDOWN));

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return Optional.of(varbinaryColumnMapping());
        }

        if (typeHandle.jdbcType() == Types.OTHER || typeHandle.jdbcType() == Types.JAVA_OBJECT) {
            return mapUnsupportedTypeToVarchar(typeHandle, jdbcTypeName);
        }

        if (session != null && TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling(session) == UnsupportedTypeHandling.CONVERT_TO_VARCHAR) {
            return mapUnsupportedTypeToVarchar(typeHandle, jdbcTypeName);
        }

        log.debug("Unsupported JDBC type: %d (%s)", typeHandle.jdbcType(), jdbcTypeName);
        return Optional.empty();
    }

    private Optional<ColumnMapping> mapUnsupportedTypeToVarchar(JdbcTypeHandle typeHandle, String jdbcTypeName) {
        Optional<ColumnMapping> mapping = mapToUnboundedVarchar(typeHandle);
        if (mapping.isPresent()) {
            log.debug("Mapping unsupported JDBC type %d (%s) to unbounded VARCHAR", typeHandle.jdbcType(), jdbcTypeName);
            return mapping;
        }

        log.debug("Falling back to manual unbounded VARCHAR mapping for unsupported JDBC type %d (%s)", typeHandle.jdbcType(), jdbcTypeName);
        return Optional.of(ColumnMapping.sliceMapping(
                createUnboundedVarcharType(),
                (resultSet, columnIndex) -> utf8Slice(resultSet.getString(columnIndex)),
                varcharWriteFunction(),
                DISABLE_PUSHDOWN));
    }

    public Optional<ParameterizedExpression> getColumnExpressionForPushdown(JdbcColumnHandle column) {
        Optional<String> castTargetType = getExecutionCastTargetType(column.getJdbcTypeHandle());
        if (castTargetType.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(new ParameterizedExpression(
                format("CAST(%s AS %s)", quoted(column.getColumnName()), castTargetType.get()),
                Collections.emptyList()));
    }

    private Optional<String> getExecutionCastTargetType(JdbcTypeHandle typeHandle) {
        int jdbcType = typeHandle.jdbcType();
        String jdbcTypeName = normalizeJdbcTypeName(typeHandle);

        if (requiresBinaryExecutionCast(jdbcType, jdbcTypeName)) {
            return Optional.of("VARBYTE(" + TERADATA_VARCHAR_MAX_LENGTH + ")");
        }
        if (requiresTextExecutionCast(jdbcType, jdbcTypeName)) {
            return Optional.of("VARCHAR(" + TERADATA_VARCHAR_MAX_LENGTH + ")");
        }
        return Optional.empty();
    }

    private String normalizeJdbcTypeName(JdbcTypeHandle typeHandle) {
        return typeHandle.jdbcTypeName()
                .orElse("")
                .toUpperCase(Locale.ROOT)
                .replace('_', ' ');
    }

    private boolean requiresBinaryExecutionCast(int jdbcType, String jdbcTypeName) {
        return jdbcType == Types.BLOB || jdbcTypeName.contains("BLOB");
    }

    private boolean requiresTextExecutionCast(int jdbcType, String jdbcTypeName) {
        if (jdbcType == Types.CLOB
                || jdbcType == Types.NCLOB
                || jdbcType == Types.NCHAR
                || jdbcType == Types.NVARCHAR
                || jdbcType == Types.LONGNVARCHAR
                || jdbcType == Types.LONGVARCHAR
                || jdbcType == Types.SQLXML
                || jdbcType == Types.DISTINCT
                || jdbcType == Types.STRUCT
                || jdbcType == Types.ARRAY
                || jdbcType == Types.OTHER
                || jdbcType == Types.JAVA_OBJECT
                || jdbcType == Types.TIME_WITH_TIMEZONE
                || jdbcType == Types.TIMESTAMP_WITH_TIMEZONE
                || jdbcType == Types.REF
                || jdbcType == Types.ROWID
                || jdbcType == Types.DATALINK) {
            return true;
        }

        return jdbcTypeName.contains("INTERVAL")
                || jdbcTypeName.contains("PERIOD")
                || jdbcTypeName.contains("JSON")
                || jdbcTypeName.contains("XML")
                || jdbcTypeName.contains("GRAPHIC")
                || jdbcTypeName.contains("GEOMETRY")
                || jdbcTypeName.contains("ARRAY")
                || jdbcTypeName.contains("UDT")
                || jdbcTypeName.contains("TIME WITH TIME ZONE")
                || jdbcTypeName.contains("TIMESTAMP WITH TIME ZONE")
                || jdbcTypeName.contains("TIME WTZ")
                || jdbcTypeName.contains("TIMESTAMP WTZ")
                || jdbcTypeName.contains("LONG VARCHAR")
                || jdbcTypeName.contains("CLOB");
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type) {
        if (type instanceof VarcharType varcharType) {
            String dataType;
            if (varcharType.isUnbounded() || varcharType.getBoundedLength() > TERADATA_VARCHAR_MAX_LENGTH) {
                dataType = "CLOB";
            } else {
                dataType = "VARCHAR(" + varcharType.getBoundedLength() + ")";
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }

        if (type instanceof CharType charType) {
            return WriteMapping.sliceMapping(
                    "CHAR(" + charType.getLength() + ")",
                    teradataCharWriteFunction());
        }

        if (type instanceof DecimalType decimalType) {
            String dataType = format("DECIMAL(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimalType));
        }

        if (type instanceof TimestampType) {
            return WriteMapping.longMapping("TIMESTAMP(6)", teradataTimestampWriteFunction());
        }

        WriteMapping writeMapping = WRITE_MAPPINGS.get(type);
        if (writeMapping != null) {
            return writeMapping;
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    @Override
    public boolean supportsAggregationPushdown(ConnectorSession session, JdbcTableHandle table, List<AggregateFunction> aggregates, java.util.Map<String, io.trino.spi.connector.ColumnHandle> assignments, List<List<io.trino.spi.connector.ColumnHandle>> groupingSets) {
        if (!TrinoExportSessionProperties.isAggregationPushdownEnabled(session)) {
            return false;
        }
        // Cannot push down aggregation if there's already a limit or sort order (TopN)
        if (table.getLimit().isPresent() || table.getSortOrder().isPresent()) {
            return false;
        }
        return true;
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments) {
        if (!TrinoExportSessionProperties.isAggregationPushdownEnabled(session)) {
            return Optional.empty();
        }
        return aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
    }

    @Override
    public Optional<ParameterizedExpression> convertPredicate(ConnectorSession session, ConnectorExpression expression, Map<String, ColumnHandle> assignments) {
        return connectorExpressionRewriter.rewrite(session, expression, assignments);
    }

    @Override
    public Optional<JdbcExpression> convertProjection(ConnectorSession session, JdbcTableHandle handle, ConnectorExpression expression, Map<String, ColumnHandle> assignments) {
        Optional<ParameterizedExpression> rewritten = connectorExpressionRewriter.rewrite(session, expression, assignments);
        if (rewritten.isEmpty()) {
            return Optional.empty();
        }
        ParameterizedExpression parameterizedExpression = rewritten.get();
        try {
            JdbcTypeHandle jdbcTypeHandle = getJdbcTypeHandle(expression.getType());
            return Optional.of(new JdbcExpression(
                    parameterizedExpression.expression(),
                    parameterizedExpression.parameters(),
                    jdbcTypeHandle
            ));
        } catch (TrinoException e) {
            log.warn("Could not determine JDBC type for projection pushdown: %s", expression.getType());
            return Optional.empty();
        }
    }

    private JdbcTypeHandle getJdbcTypeHandle(Type type) {
        if (type == BIGINT) {
            return new JdbcTypeHandle(Types.BIGINT, Optional.of("BIGINT"), Optional.of(19), Optional.empty(), Optional.empty(), Optional.empty());
        }
        if (type == INTEGER) {
            return new JdbcTypeHandle(Types.INTEGER, Optional.of("INTEGER"), Optional.of(10), Optional.empty(), Optional.empty(), Optional.empty());
        }
        if (type == SMALLINT) {
            return new JdbcTypeHandle(Types.SMALLINT, Optional.of("SMALLINT"), Optional.of(5), Optional.empty(), Optional.empty(), Optional.empty());
        }
        if (type == TINYINT) {
            return new JdbcTypeHandle(Types.TINYINT, Optional.of("BYTEINT"), Optional.of(3), Optional.empty(), Optional.empty(), Optional.empty());
        }
        if (type == DOUBLE) {
            return new JdbcTypeHandle(Types.DOUBLE, Optional.of("FLOAT"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        }
        if (type == REAL) {
            return new JdbcTypeHandle(Types.REAL, Optional.of("REAL"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            return new JdbcTypeHandle(Types.DECIMAL, Optional.of("DECIMAL"), Optional.of(decimalType.getPrecision()), Optional.of(decimalType.getScale()), Optional.empty(), Optional.empty());
        }
        if (type == DATE) {
            return new JdbcTypeHandle(Types.DATE, Optional.of("DATE"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        }
        // Add more types as needed
        throw new TrinoException(NOT_SUPPORTED, "Unsupported type for projection pushdown: " + type);
    }

    @Override
    public boolean supportsTopN(ConnectorSession session, JdbcTableHandle table, List<JdbcSortItem> sortOrder) {
        log.info("Enabling TopN pushdown for table: %s", table);
        return true;
    }

    @Override
    public boolean isTopNGuaranteed(ConnectorSession session) {
        return true;
    }

    @Override
    protected Optional<TopNFunction> topNFunction() {
        // Return original SQL; wrapping is handled in TeradataQueryBuilder
        return Optional.of((sql, sortItems, limit) -> sql);
    }

    @Override
    public boolean supportsLimit() {
        return true;
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session) {
        return true;
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction() {
        // Return original SQL; wrapping is handled in TeradataQueryBuilder
        return Optional.of((sql, limit) -> sql);
    }

    // Teradata-specific type handle for aggregates
    private static Optional<JdbcTypeHandle> toTypeHandle(DecimalType decimalType) {
        return Optional.of(new JdbcTypeHandle(
                Types.DECIMAL,
                Optional.of("DECIMAL"),
                Optional.of(decimalType.getPrecision()),
                Optional.of(decimalType.getScale()),
                Optional.empty(),
                Optional.empty()));
    }

    // Teradata-specific read/write functions
    private static LongReadFunction teradataDateReadFunction() {
        return (resultSet, columnIndex) -> {
            java.sql.Date date = resultSet.getDate(columnIndex);
            return date.toLocalDate().toEpochDay();
        };
    }

    private static LongWriteFunction teradataDateWriteFunction() {
        return LongWriteFunction.of(Types.DATE, (statement, index, value) -> {
            LocalDate date = LocalDate.ofEpochDay(value);
            statement.setDate(index, java.sql.Date.valueOf(date));
        });
    }

    private static LongReadFunction teradataTimestampReadFunction() {
        return (resultSet, columnIndex) -> {
            java.sql.Timestamp ts = resultSet.getTimestamp(columnIndex);
            return ts.toLocalDateTime().toEpochSecond(ZoneOffset.UTC) * 1_000_000 
                   + ts.toLocalDateTime().getNano() / 1000;
        };
    }

    private static LongWriteFunction teradataTimestampWriteFunction() {
        return LongWriteFunction.of(Types.TIMESTAMP, (statement, index, epochMicros) -> {
            long epochSeconds = epochMicros / 1_000_000;
            int nanoAdjustment = (int) ((epochMicros % 1_000_000) * 1000);
            LocalDateTime ldt = LocalDateTime.ofEpochSecond(epochSeconds, nanoAdjustment, ZoneOffset.UTC);
            statement.setString(index, "TIMESTAMP '" + TIMESTAMP_FORMATTER.format(ldt) + "'");
        });
    }

    private static LongReadFunction teradataTimeReadFunction() {
        return (resultSet, columnIndex) -> {
            java.sql.Time time = resultSet.getTime(columnIndex);
            if (time == null) {
                return 0L;
            }
            return time.toLocalTime().toNanoOfDay() / 1000;
        };
    }

    public static ColumnMapping varbinaryColumnMapping() {
        return ColumnMapping.sliceMapping(
                VARBINARY,
                (resultSet, columnIndex) -> {
                    byte[] bytes = resultSet.getBytes(columnIndex);
                    return bytes == null ? null : io.airlift.slice.Slices.wrappedBuffer(bytes);
                },
                varbinaryWriteFunction(),
                FULL_PUSHDOWN);
    }

    public static SliceWriteFunction varbinaryWriteFunction() {
        return (statement, index, value) -> statement.setBytes(index, value.getBytes());
    }

    private static LongWriteFunction teradataTimeWriteFunction() {
        return LongWriteFunction.of(Types.TIME, (statement, index, micros) -> {
            java.time.LocalTime lt = java.time.LocalTime.ofNanoOfDay(micros * 1000);
            statement.setString(index, "TIME '" + lt.toString() + "'");
        });
    }

    private static SliceWriteFunction teradataCharWriteFunction() {
        return SliceWriteFunction.of(Types.CHAR, (statement, index, value) -> 
                statement.setString(index, value.toStringUtf8()));
    }

    // Fast metadata cache using old direct JDBC approach (bypasses slow BaseJdbcClient queries)
    private java.util.Map<io.trino.spi.connector.SchemaTableName, List<JdbcColumnHandle>> metadataCache;
    
    private java.util.Map<io.trino.spi.connector.SchemaTableName, List<JdbcColumnHandle>> createMetadataCache() {
        final int maxSize = this.metadataCacheSize;
        return Collections.synchronizedMap(new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry eldest) {
                return size() > maxSize;
            }
        });
    }

    /**
     * Update the metadata cache with column handles for a table.
     * Called by MetadataRefreshService during background refresh.
     */
    public void updateMetadataCache(io.trino.spi.connector.SchemaTableName tableName, List<JdbcColumnHandle> columns) {
        if (columns != null && !columns.isEmpty()) {
            metadataCache.put(tableName, columns);
            log.debug("Updated metadata cache for table %s with %d columns", tableName, columns.size());
        }
    }

    /**
     * Get the connection factory for background metadata refresh.
     */
    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, io.trino.spi.connector.SchemaTableName tableName, io.trino.plugin.jdbc.RemoteTableName remoteTableName) {
        // Check cache first
        List<JdbcColumnHandle> cached = metadataCache.get(tableName);
        if (cached != null) {
            log.debug("Fast metadata cache HIT for %s", tableName);
            return cached;
        }

        log.info("Fast metadata cache MISS for %s - fetching via direct JDBC", tableName);
        
        try (Connection conn = connectionFactory.openConnection(session)) {
            List<JdbcColumnHandle> columns = getColumns(conn, session, tableName, remoteTableName);
            
            // Cache the result if found via direct fetch
            if (!columns.isEmpty()) {
                metadataCache.put(tableName, new ArrayList<>(columns));
                return columns;
            } else {
                 log.warn("Direct metadata fetch returned no columns for %s - falling back to standard JDBC", tableName);
                 List<JdbcColumnHandle> fallbackColumns = super.getColumns(session, tableName, remoteTableName);
                 if (!fallbackColumns.isEmpty()) {
                     metadataCache.put(tableName, fallbackColumns);
                 }
                 return fallbackColumns;
            }
        } catch (TrinoException e) {
            throw e;
        } catch (SQLException e) {
            log.error(e, "Error fetching columns for %s - falling back to standard JDBC", tableName);
            List<JdbcColumnHandle> fallbackColumns = super.getColumns(session, tableName, remoteTableName);
            if (!fallbackColumns.isEmpty()) {
                metadataCache.put(tableName, fallbackColumns);
            }
            return fallbackColumns;
        }
    }

    /**
     * Overload for getColumns that uses an existing connection.
     * Useful for background metadata refresh where session is null.
     */
    public List<JdbcColumnHandle> getColumns(Connection conn, ConnectorSession session, io.trino.spi.connector.SchemaTableName tableName, io.trino.plugin.jdbc.RemoteTableName remoteTableName) {
        List<JdbcColumnHandle> columns = new ArrayList<>();
        
        // Try direct JDBC metadata query (much faster than BaseJdbcClient approach)
        fetchColumnsDirectly(conn, tableName.getSchemaName(), tableName.getTableName(), columns, session);
        
        // Try uppercase if no results
        if (columns.isEmpty()) {
            fetchColumnsDirectly(conn, tableName.getSchemaName(), tableName.getTableName().toUpperCase(), columns, session);
        }
        
        // Try both uppercase if still no results
        if (columns.isEmpty()) {
            fetchColumnsDirectly(conn, tableName.getSchemaName().toUpperCase(), tableName.getTableName().toUpperCase(), columns, session);
        }

        // Try Upper Schema + Exact Table
        if (columns.isEmpty()) {
            fetchColumnsDirectly(conn, tableName.getSchemaName().toUpperCase(), tableName.getTableName(), columns, session);
        }
        
        return columns;
    }

    private void fetchColumnsDirectly(Connection conn, String schema, String table, List<JdbcColumnHandle> columns, ConnectorSession session) {
        // Option #4: SELECT * FROM <dbname>.<tablename> WHERE 1=0
        // This is significantly faster for Teradata (1.2s vs 31s for large tables) compared to standard JDBC getColumns
        String sql = format("SELECT * FROM \"%s\".\"%s\" WHERE 1=0", schema, table);
        
        try (java.sql.Statement stmt = conn.createStatement()) {
            // Set Proxy User if session is available
            if (session != null) {
                String proxyUserSql = format("SET QUERY_BAND = 'ProxyUser=%s;' FOR SESSION", session.getUser());
                try {
                    stmt.execute(proxyUserSql);
                } catch (SQLException e) {
                    log.warn("Failed to set proxy user '%s': %s", session.getUser(), e.getMessage());
                }
            }

            try (ResultSet rs = stmt.executeQuery(sql)) {
                java.sql.ResultSetMetaData meta = rs.getMetaData();
                int columnCount = meta.getColumnCount();
                
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = meta.getColumnName(i);
                    int dataType = meta.getColumnType(i);
                    int columnSize = meta.getPrecision(i);
                    int decimalDigits = meta.getScale(i);
                    String typeName = meta.getColumnTypeName(i);
                    
                    // Create JdbcTypeHandle
                    JdbcTypeHandle typeHandle = new JdbcTypeHandle(
                        dataType,
                        Optional.ofNullable(typeName),
                        Optional.of(columnSize),
                        Optional.of(decimalDigits),
                        Optional.empty(),
                        Optional.empty()
                    );
                    
                    // Map to column mapping
                    Optional<ColumnMapping> columnMapping = toColumnMapping(session, conn, typeHandle);
                    if (columnMapping.isPresent()) {
                        boolean isNullable = meta.isNullable(i) != java.sql.ResultSetMetaData.columnNoNulls;
                        columns.add(JdbcColumnHandle.builder()
                                .setColumnName(columnName.toLowerCase())
                                .setJdbcTypeHandle(typeHandle)
                                .setColumnType(columnMapping.get().getType())
                                .setNullable(isNullable)
                                .build());
                    }
                }
            }
        } catch (SQLException e) {
            // Surface the actual Teradata error if it's NOT a "not found" error.
            // 3807: Object not found, 3802: Database not found.
            // These are expected during case-insensitive probing.
            if (e.getErrorCode() != 3807 && e.getErrorCode() != 3802) {
                throw new TrinoException(PERMISSION_DENIED, "Teradata metadata fetch failed for SQL '" + sql + "': " + e.getMessage(), e);
            }
            // Swallow "not found" to allow retry logic in caller to proceed
            log.debug("Direct metadata fetch candidate not found for SQL '%s': %s", sql, e.getMessage());
        } finally {
            // Unset Proxy User
            try (java.sql.Statement stmt = conn.createStatement()) {
                stmt.execute("SET QUERY_BAND = NONE FOR SESSION");
            } catch (SQLException e) {
                 log.warn("Failed to reset proxy user: %s", e.getMessage());
            }
        }
    }
    @Override
    public Optional<JdbcTableHandle> getTableHandle(ConnectorSession session, io.trino.spi.connector.SchemaTableName schemaTableName) {
        Optional<JdbcTableHandle> handle = super.getTableHandle(session, schemaTableName);
        if (handle.isPresent()) {
            return handle;
        }

        try (Connection conn = connectionFactory.openConnection(session)) {
            List<JdbcColumnHandle> columns = new ArrayList<>();
            String schema = schemaTableName.getSchemaName();
            String table = schemaTableName.getTableName();
            
            // 1. Try Upper Table
            fetchColumnsDirectly(conn, schema, table.toUpperCase(), columns, session);
            if (!columns.isEmpty()) {
                // Optimization: Cache columns since we already fetched them!
                metadataCache.put(schemaTableName, new ArrayList<>(columns));
                return Optional.of(new JdbcTableHandle(
                    schemaTableName,
                    new io.trino.plugin.jdbc.RemoteTableName(Optional.empty(), Optional.of(schema), table.toUpperCase()),
                    Optional.empty()));
            }

            // 2. Try Upper Schema + Upper Table
            columns.clear();
            fetchColumnsDirectly(conn, schema.toUpperCase(), table.toUpperCase(), columns, session);
            if (!columns.isEmpty()) {
                metadataCache.put(schemaTableName, new ArrayList<>(columns));
                return Optional.of(new JdbcTableHandle(
                    schemaTableName,
                    new io.trino.plugin.jdbc.RemoteTableName(Optional.empty(), Optional.of(schema.toUpperCase()), table.toUpperCase()),
                    Optional.empty()));
            }

            // 3. Try Upper Schema + Exact Table
            columns.clear();
            fetchColumnsDirectly(conn, schema.toUpperCase(), table, columns, session);
            if (!columns.isEmpty()) {
                metadataCache.put(schemaTableName, new ArrayList<>(columns));
                return Optional.of(new JdbcTableHandle(
                    schemaTableName,
                    new io.trino.plugin.jdbc.RemoteTableName(Optional.empty(), Optional.of(schema.toUpperCase()), table),
                    Optional.empty()));
            }
        } catch (SQLException e) {
            log.warn("Failed to check table existence via columns fallback: %s", e.getMessage());
        }
        return Optional.empty();
    }
}
