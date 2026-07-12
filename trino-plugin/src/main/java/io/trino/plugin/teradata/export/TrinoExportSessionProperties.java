package io.trino.plugin.teradata.export;

import com.google.common.collect.ImmutableList;
import io.trino.spi.session.PropertyMetadata;

import com.google.inject.Inject;
import java.util.List;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;

/**
 * Session properties for Teradata Export Connector.
 * 
 * Includes standard JDBC pushdown properties required by DefaultJdbcMetadata.
 */
public class TrinoExportSessionProperties {
    public static final String AGGREGATION_PUSHDOWN_ENABLED = "aggregation_pushdown_enabled";
    public static final String JOIN_PUSHDOWN_ENABLED = "join_pushdown_enabled";
    public static final String TOPN_PUSHDOWN_ENABLED = "topn_pushdown_enabled";
    public static final String SYSTEM_QUERY_ENABLED = "system_query_enabled";
    public static final String COMPLEX_EXPRESSION_PUSHDOWN = "complex_expression_pushdown";
    public static final String BULK_LIST_COLUMNS = "bulk_list_columns";

    public static final String COMPLEX_JOIN_PUSHDOWN_ENABLED = "complex_join_pushdown_enabled";
    public static final String DOMAIN_COMPACTION_THRESHOLD = "domain_compaction_threshold";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public TrinoExportSessionProperties(TrinoExportConfig config, io.trino.plugin.jdbc.TypeHandlingJdbcConfig typeHandlingJdbcConfig) {
        this.sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(booleanProperty(
                        AGGREGATION_PUSHDOWN_ENABLED,
                        "Enable aggregation pushdown",
                        config.isEnableAggregationPushdown(),
                        false))
                .add(booleanProperty(
                        JOIN_PUSHDOWN_ENABLED,
                        "Enable join pushdown",
                        config.isEnableJoinPushdown(),
                        false))
                .add(booleanProperty(
                        COMPLEX_JOIN_PUSHDOWN_ENABLED,
                        "Enable complex join pushdown",
                        config.isEnableComplexJoinPushdown(),
                        false))
                .add(booleanProperty(
                        TOPN_PUSHDOWN_ENABLED,
                        "Enable TopN pushdown",
                        config.isEnableTopNPushdown(),
                        false))
                .add(booleanProperty(
                    SYSTEM_QUERY_ENABLED,
                    "Enable system.query table function",
                    config.isSystemQueryEnabled(),
                    false))
                .add(booleanProperty(
                        COMPLEX_EXPRESSION_PUSHDOWN,
                        "Enable complex expression pushdown",
                        config.isEnableComplexExpressionPushdown(),
                        false))
                .add(booleanProperty(
                        BULK_LIST_COLUMNS,
                        "Enable bulk list columns",
                        false,
                        false))
                .add(io.trino.spi.session.PropertyMetadata.integerProperty(
                        DOMAIN_COMPACTION_THRESHOLD,
                        "Domain compaction threshold",
                        config.getDomainCompactionThreshold(),
                        false))
                .addAll(new io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties(typeHandlingJdbcConfig).getSessionProperties())
                .build();
    }

    public List<PropertyMetadata<?>> getSessionProperties() {
        return sessionProperties;
    }

    public static boolean isAggregationPushdownEnabled(io.trino.spi.connector.ConnectorSession session) {
        return session.getProperty(AGGREGATION_PUSHDOWN_ENABLED, Boolean.class);
    }

    public static boolean isJoinPushdownEnabled(io.trino.spi.connector.ConnectorSession session) {
        return session.getProperty(JOIN_PUSHDOWN_ENABLED, Boolean.class);
    }

    public static boolean isComplexJoinPushdownEnabled(io.trino.spi.connector.ConnectorSession session) {
        return session.getProperty(COMPLEX_JOIN_PUSHDOWN_ENABLED, Boolean.class);
    }

    public static boolean isTopNPushdownEnabled(io.trino.spi.connector.ConnectorSession session) {
        return session.getProperty(TOPN_PUSHDOWN_ENABLED, Boolean.class);
    }

    public static boolean isSystemQueryEnabled(io.trino.spi.connector.ConnectorSession session) {
        return session.getProperty(SYSTEM_QUERY_ENABLED, Boolean.class);
    }

    public static boolean isComplexExpressionPushdown(io.trino.spi.connector.ConnectorSession session) {
        return session.getProperty(COMPLEX_EXPRESSION_PUSHDOWN, Boolean.class);
    }

    public static int getDomainCompactionThreshold(io.trino.spi.connector.ConnectorSession session) {
        return session.getProperty(DOMAIN_COMPACTION_THRESHOLD, Integer.class);
    }
}
