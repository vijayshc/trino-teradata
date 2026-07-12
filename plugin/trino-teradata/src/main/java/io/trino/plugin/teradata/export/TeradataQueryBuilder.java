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

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;

/**
 * Teradata-specific QueryBuilder extending Trino's DefaultQueryBuilder.
 * 
 * This class leverages Trino's SQL generation infrastructure.
 * The principal Teradata-specific addition is the Table Operator UDF wrapping.
 */
public class TeradataQueryBuilder extends DefaultQueryBuilder {
    private static final Logger log = Logger.get(TeradataQueryBuilder.class);

    @Inject
    public TeradataQueryBuilder(RemoteQueryModifier queryModifier) {
        super(queryModifier);
        log.info("TeradataQueryBuilder initialized");
    }

    /**
     * Builds the final SQL to be executed on Teradata, wrapping the base query
     * with the ExportToTrino Table Operator UDF.
     * 
     * Optionally wraps the base query with TOP/SAMPLE for limit pushdown.
     */
    public String buildExportQuery(
            String baseQuery,
            String udfDatabase,
            String udfName,
            String targetIps,
            String queryId,
            String token,
            int batchSize,
            String compressionAlgorithm,
            java.util.OptionalLong limit,
            java.util.Optional<java.util.List<io.trino.plugin.jdbc.JdbcSortItem>> sortOrder) {
        
        // Wrap base query with TOP/SAMPLE if limit is present
        String wrappedQuery = baseQuery;
        if (limit.isPresent()) {
            long limitValue = limit.getAsLong();
            
            if (sortOrder.isPresent() && !sortOrder.get().isEmpty()) {
                // TopN: SELECT TOP N ... ORDER BY ...
                StringBuilder orderByClause = new StringBuilder();
                java.util.List<io.trino.plugin.jdbc.JdbcSortItem> sortItems = sortOrder.get();
                for (int i = 0; i < sortItems.size(); i++) {
                    if (i > 0) orderByClause.append(", ");
                    io.trino.plugin.jdbc.JdbcSortItem sortItem = sortItems.get(i);
                    // JdbcSortItem is a record with public fields: column() and sortOrder()
                    orderByClause.append(sortItem.column().getColumnName());
                    // sortOrder() returns SortOrder enum
                    boolean isAscending = sortItem.sortOrder().isAscending();
                    orderByClause.append(isAscending ? " ASC" : " DESC");
                }
                wrappedQuery = String.format("SELECT TOP %d * FROM (%s) o ORDER BY %s",
                    limitValue, baseQuery, orderByClause.toString());
                log.info("Applied TopN pushdown: TOP %d ORDER BY %s", limitValue, orderByClause.toString());
            } else {
                // Plain LIMIT: SELECT * FROM (...) SAMPLE N
                wrappedQuery = String.format("SELECT * FROM (%s) o SAMPLE %d", baseQuery, limitValue);
                log.info("Applied LIMIT pushdown: SAMPLE %d", limitValue);
            }
        }
        
        return String.format(
                "SELECT * FROM %s.%s(" +
                "  ON (%s)" +
                "   ON (SELECT CAST('%s' AS VARCHAR(2048)) as target_ips, " +
                "CAST('%s' AS VARCHAR(256)) as qid, " +
                "CAST('%s' AS VARCHAR(256)) as token, " +
                "CAST(%d AS INTEGER) as batch_size, " +
                "CAST('%s' AS VARCHAR(20)) as compression_algorithm) DIMENSION" +
                ") AS export_result",
                udfDatabase, udfName, wrappedQuery, targetIps, queryId, token, batchSize, compressionAlgorithm);
    }

    public String quote(String name) {
        if (name == null) return null;
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }

    public String maybeQuote(String name) {
        if (name == null) return null;
        return quote(name);
    }

    public String formatValueForSql(Object value, io.trino.spi.type.Type type) {
        if (value == null) {
            return "NULL";
        }

        if (type instanceof io.trino.spi.type.VarcharType || type instanceof io.trino.spi.type.CharType) {
            String strValue;
            if (value instanceof io.airlift.slice.Slice slice) {
                strValue = slice.toStringUtf8();
            } else {
                strValue = value.toString();
            }
            return "'" + strValue.replace("'", "''") + "'";
        }

        if (type instanceof io.trino.spi.type.IntegerType || type instanceof io.trino.spi.type.BigintType ||
            type instanceof io.trino.spi.type.SmallintType || type instanceof io.trino.spi.type.TinyintType) {
            return value.toString();
        }

        if (type instanceof io.trino.spi.type.DoubleType || type instanceof io.trino.spi.type.RealType) {
            return value.toString();
        }

        if (type instanceof io.trino.spi.type.DecimalType decimalType) {
            if (value instanceof Long l) {
                return io.trino.spi.type.Decimals.toString(l, decimalType.getScale());
            } else if (value instanceof io.trino.spi.type.Int128 bi) {
                return io.trino.spi.type.Decimals.toString(bi, decimalType.getScale());
            }
            return value.toString();
        }

        if (type instanceof io.trino.spi.type.BooleanType) {
            return ((Boolean) value) ? "1" : "0";
        }

        if (type instanceof io.trino.spi.type.DateType) {
            long epochDays = (Long) value;
            java.time.LocalDate date = java.time.LocalDate.ofEpochDay(epochDays);
            return "DATE '" + date.toString() + "'";
        }

        if (type instanceof io.trino.spi.type.TimestampType) {
            long epochMicros = (Long) value;
            long epochSeconds = epochMicros / 1_000_000;
            int nanoAdjustment = (int) ((epochMicros % 1_000_000) * 1000);
            java.time.LocalDateTime dt = java.time.LocalDateTime.ofEpochSecond(epochSeconds, nanoAdjustment, java.time.ZoneOffset.UTC);
            return "TIMESTAMP '" + dt.format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")) + "'";
        }

        return value.toString();
    }

    public String getQualifiedColumnName(TrinoExportTableHandle tableHandle, TrinoExportColumnHandle columnHandle) {
        return columnHandle.getQualifiedName(this);
    }
}
