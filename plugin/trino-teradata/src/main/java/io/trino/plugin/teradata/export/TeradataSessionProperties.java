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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;

/**
 * Session properties for Teradata connector.
 * These can be set per-session to override catalog defaults.
 */
public final class TeradataSessionProperties implements SessionPropertiesProvider {
    
    public static final String PUSHDOWN_ENABLED = "pushdown_enabled";
    public static final String AGGREGATION_PUSHDOWN_ENABLED = "aggregation_pushdown_enabled";
    public static final String TOPN_PUSHDOWN_ENABLED = "topn_pushdown_enabled";
    public static final String JOIN_PUSHDOWN_ENABLED = "join_pushdown_enabled";
    public static final String DOMAIN_COMPACTION_THRESHOLD = "domain_compaction_threshold";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public TeradataSessionProperties(TrinoExportConfig config) {
        sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(booleanProperty(
                        PUSHDOWN_ENABLED,
                        "Enable predicate pushdown to Teradata",
                        true,
                        false))
                .add(booleanProperty(
                        AGGREGATION_PUSHDOWN_ENABLED,
                        "Enable aggregation pushdown to Teradata",
                        config.isEnableAggregationPushdown(),
                        false))
                .add(booleanProperty(
                        TOPN_PUSHDOWN_ENABLED,
                        "Enable Top-N pushdown to Teradata",
                        config.isEnableTopNPushdown(),
                        false))
                .add(booleanProperty(
                        JOIN_PUSHDOWN_ENABLED,
                        "Enable join pushdown to Teradata",
                        false,
                        false))
                .add(integerProperty(
                        DOMAIN_COMPACTION_THRESHOLD,
                        "Maximum number of discrete values in IN list for pushdown",
                        1000,
                        false))
                .build();
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties() {
        return sessionProperties;
    }

    public static boolean isPushdownEnabled(ConnectorSession session) {
        return session.getProperty(PUSHDOWN_ENABLED, Boolean.class);
    }

    public static boolean isAggregationPushdownEnabled(ConnectorSession session) {
        return session.getProperty(AGGREGATION_PUSHDOWN_ENABLED, Boolean.class);
    }

    public static boolean isTopNPushdownEnabled(ConnectorSession session) {
        return session.getProperty(TOPN_PUSHDOWN_ENABLED, Boolean.class);
    }

    public static boolean isJoinPushdownEnabled(ConnectorSession session) {
        return session.getProperty(JOIN_PUSHDOWN_ENABLED, Boolean.class);
    }

    public static int getDomainCompactionThreshold(ConnectorSession session) {
        return session.getProperty(DOMAIN_COMPACTION_THRESHOLD, Integer.class);
    }
}
