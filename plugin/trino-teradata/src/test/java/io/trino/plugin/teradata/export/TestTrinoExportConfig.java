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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Lightweight defaults tests. Full ConfigAssertions-style coverage can be added
 * when airlift configuration-testing is pulled in as a test dependency.
 */
public class TestTrinoExportConfig
{
    @Test
    public void testDefaults()
    {
        TrinoExportConfig config = new TrinoExportConfig();
        assertThat(config.getBridgePort()).isEqualTo(9999);
        assertThat(config.getBatchSize()).isEqualTo(500_000);
        assertThat(config.isEnforceProxyAuthentication()).isTrue();
        assertThat(config.getUdfDatabase()).isEqualTo("TrinoExport");
        assertThat(config.getUdfName()).isEqualTo("ExportToTrino");
        assertThat(config.getTrinoAddress()).isEqualTo("localhost");
        assertThat(config.isEnableAggregationPushdown()).isTrue();
        assertThat(config.isEnableTopNPushdown()).isTrue();
        assertThat(config.isEnableDynamicFiltering()).isTrue();
    }

    @Test
    public void testFluentSetters()
    {
        TrinoExportConfig config = new TrinoExportConfig()
                .setBridgePort(10001)
                .setBatchSize(1000)
                .setUdfDatabase("MyDb")
                .setEnforceProxyAuthentication(false);

        assertThat(config.getBridgePort()).isEqualTo(10001);
        assertThat(config.getBatchSize()).isEqualTo(1000);
        assertThat(config.getUdfDatabase()).isEqualTo("MyDb");
        assertThat(config.isEnforceProxyAuthentication()).isFalse();
    }
}
