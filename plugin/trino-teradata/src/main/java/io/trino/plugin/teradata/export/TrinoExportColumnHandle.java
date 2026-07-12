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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;

import java.util.Objects;

public class TrinoExportColumnHandle implements ColumnHandle {
    private final String name;
    private final Type type;
    private final int ordinal;
    private final io.trino.spi.connector.SchemaTableName table;
    private final java.util.Optional<String> alias;

    @JsonCreator
    public TrinoExportColumnHandle(
            @JsonProperty("name") String name,
            @JsonProperty("type") Type type,
            @JsonProperty("ordinal") int ordinal,
            @JsonProperty("table") io.trino.spi.connector.SchemaTableName table,
            @JsonProperty("alias") java.util.Optional<String> alias) {
        this.name = name;
        this.type = type;
        this.ordinal = ordinal;
        this.table = table;
        this.alias = alias != null ? alias : java.util.Optional.empty();
    }

    // Backward compatibility constructor
    public TrinoExportColumnHandle(String name, Type type, int ordinal, io.trino.spi.connector.SchemaTableName table) {
        this(name, type, ordinal, table, java.util.Optional.empty());
    }

    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public Type getType() {
        return type;
    }

    @JsonProperty
    public int getOrdinal() {
        return ordinal;
    }

    @JsonProperty
    public io.trino.spi.connector.SchemaTableName getTable() {
        return table;
    }

    @JsonProperty
    public java.util.Optional<String> getAlias() {
        return alias;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TrinoExportColumnHandle that = (TrinoExportColumnHandle) o;
        return Objects.equals(name, that.name) &&
               Objects.equals(table, that.table) &&
               Objects.equals(alias, that.alias);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, table, alias);
    }

    @Override
    public String toString() {
        return alias.map(a -> a + ".").orElse("") + name + ":" + type;
    }

    /**
     * Get the qualified name for SQL generation (e.g., t0."column_name").
     */
    public String getQualifiedName(TeradataQueryBuilder queryBuilder) {
        String quotedName = queryBuilder.quote(name);
        return alias.map(a -> queryBuilder.quote(a) + "." + quotedName)
                    .orElse(quotedName);
    }
}
