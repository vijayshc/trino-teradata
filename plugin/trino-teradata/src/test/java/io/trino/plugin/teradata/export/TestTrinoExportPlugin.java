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
import io.trino.spi.connector.ConnectorFactory;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestTrinoExportPlugin
{
    @Test
    public void testGetConnectorFactories()
    {
        TrinoExportPlugin plugin = new TrinoExportPlugin();
        Iterable<ConnectorFactory> factories = plugin.getConnectorFactories();
        assertThat(factories).isInstanceOf(ImmutableList.class);
        assertThat(factories).hasSize(1);
        assertThat(factories.iterator().next().getName()).isEqualTo("teradata_export");
    }
}
