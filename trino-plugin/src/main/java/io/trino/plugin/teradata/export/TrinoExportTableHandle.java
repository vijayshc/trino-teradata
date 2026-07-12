package io.trino.plugin.teradata.export;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Table handle that stores optimization information:
 * - projectedColumns: columns selected by Trino (column pruning)
 * - predicateClause: WHERE clause to push down to Teradata
 * - limit: LIMIT clause to push down to Teradata (using TOP N or SAMPLE)
 * - sortOrder: ORDER BY clause for Top-N pushdown (ORDER BY ... LIMIT N)
 * - aggregation: Aggregation pushdown information (aggregate functions + GROUP BY)
 */
public class TrinoExportTableHandle implements ConnectorTableHandle {
    private final SchemaTableName schemaTableName;
    private final Optional<List<String>> projectedColumns;
    private final Optional<String> predicateClause;
    private final OptionalLong limit;
    private final Optional<List<SortItem>> sortOrder;
    private final Optional<AggregationInfo> aggregation;
    private final Optional<String> fromClause;  // Complete FROM clause for joins (e.g., "t0.table1 t0 JOIN t1.table2 t1 ON t0.id = t1.id")
    private final int nextTableAlias;           // Next alias counter for multi-way joins (t0, t1, t2...)
    private final Optional<java.util.Map<String, TrinoExportColumnHandle>> assignments;
    private final Optional<List<TrinoExportColumnHandle>> joinColumns;

    public TrinoExportTableHandle(SchemaTableName schemaTableName) {
        this(schemaTableName, Optional.empty(), Optional.empty(), OptionalLong.empty(), Optional.empty(), Optional.empty(), Optional.empty(), 0, Optional.empty(), Optional.empty());
    }

    @JsonCreator
    public TrinoExportTableHandle(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("projectedColumns") Optional<List<String>> projectedColumns,
            @JsonProperty("predicateClause") Optional<String> predicateClause,
            @JsonProperty("limit") OptionalLong limit,
            @JsonProperty("sortOrder") Optional<List<SortItem>> sortOrder,
            @JsonProperty("aggregation") Optional<AggregationInfo> aggregation,
            @JsonProperty("fromClause") Optional<String> fromClause,
            @JsonProperty("nextTableAlias") int nextTableAlias,
            @JsonProperty("joinColumns") Optional<List<TrinoExportColumnHandle>> joinColumns,
            @JsonProperty("assignments") Optional<java.util.Map<String, TrinoExportColumnHandle>> assignments) {
        this.schemaTableName = schemaTableName;
        this.projectedColumns = projectedColumns;
        this.predicateClause = predicateClause;
        this.limit = limit;
        this.sortOrder = sortOrder;
        this.aggregation = aggregation;
        this.fromClause = fromClause != null ? fromClause : Optional.empty();
        this.nextTableAlias = nextTableAlias;
        this.joinColumns = joinColumns != null ? joinColumns : Optional.empty();
        this.assignments = assignments != null ? assignments : Optional.empty();
    }

    // Backward compatibility constructor (6 params - without join)
    public TrinoExportTableHandle(
            SchemaTableName schemaTableName,
            Optional<List<String>> projectedColumns,
            Optional<String> predicateClause,
            OptionalLong limit,
            Optional<List<SortItem>> sortOrder,
            Optional<AggregationInfo> aggregation) {
        this(schemaTableName, projectedColumns, predicateClause, limit, sortOrder, aggregation, Optional.empty(), 0, Optional.empty(), Optional.empty());
    }

    // Backward compatibility constructor (5 params)
    public TrinoExportTableHandle(
            SchemaTableName schemaTableName,
            Optional<List<String>> projectedColumns,
            Optional<String> predicateClause,
            OptionalLong limit,
            Optional<List<SortItem>> sortOrder) {
        this(schemaTableName, projectedColumns, predicateClause, limit, sortOrder, Optional.empty());
    }

    // Backward compatibility constructor (4 params)
    public TrinoExportTableHandle(
            SchemaTableName schemaTableName,
            Optional<List<String>> projectedColumns,
            Optional<String> predicateClause,
            OptionalLong limit) {
        this(schemaTableName, projectedColumns, predicateClause, limit, Optional.empty(), Optional.empty());
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName() {
        return schemaTableName;
    }

    @JsonProperty
    public Optional<List<String>> getProjectedColumns() {
        return projectedColumns;
    }

    @JsonProperty
    public Optional<String> getPredicateClause() {
        return predicateClause;
    }

    @JsonProperty
    public OptionalLong getLimit() {
        return limit;
    }

    @JsonProperty
    public Optional<List<SortItem>> getSortOrder() {
        return sortOrder;
    }

    @JsonProperty
    public Optional<AggregationInfo> getAggregation() {
        return aggregation;
    }

    @JsonProperty
    public Optional<List<TrinoExportColumnHandle>> getJoinColumns() {
        return joinColumns;
    }

    @JsonProperty
    public Optional<java.util.Map<String, TrinoExportColumnHandle>> getAssignments() {
        return assignments;
    }

    public TrinoExportTableHandle withProjectedColumns(List<String> columns) {
        return new TrinoExportTableHandle(schemaTableName, Optional.of(columns), predicateClause, limit, sortOrder, aggregation, fromClause, nextTableAlias, joinColumns, assignments);
    }

    public TrinoExportTableHandle withPredicateClause(String predicate) {
        return new TrinoExportTableHandle(schemaTableName, projectedColumns, Optional.of(predicate), limit, sortOrder, aggregation, fromClause, nextTableAlias, joinColumns, assignments);
    }

    public TrinoExportTableHandle withLimit(long limit) {
        return new TrinoExportTableHandle(schemaTableName, projectedColumns, predicateClause, OptionalLong.of(limit), sortOrder, aggregation, fromClause, nextTableAlias, joinColumns, assignments);
    }

    public TrinoExportTableHandle withSortOrder(List<SortItem> sortOrder) {
        return new TrinoExportTableHandle(schemaTableName, projectedColumns, predicateClause, limit, Optional.of(sortOrder), aggregation, fromClause, nextTableAlias, joinColumns, assignments);
    }

    public TrinoExportTableHandle withTopN(List<SortItem> sortOrder, long limit) {
        return new TrinoExportTableHandle(schemaTableName, projectedColumns, predicateClause, OptionalLong.of(limit), Optional.of(sortOrder), aggregation, fromClause, nextTableAlias, joinColumns, assignments);
    }

    public TrinoExportTableHandle withAggregation(AggregationInfo aggregation) {
        return new TrinoExportTableHandle(schemaTableName, projectedColumns, predicateClause, limit, sortOrder, Optional.of(aggregation), fromClause, nextTableAlias, joinColumns, assignments);
    }

    public TrinoExportTableHandle withFromClause(String newFromClause, int newNextAlias) {
        return new TrinoExportTableHandle(schemaTableName, projectedColumns, predicateClause, limit, sortOrder, aggregation, Optional.of(newFromClause), newNextAlias, joinColumns, assignments);
    }

    public TrinoExportTableHandle withJoinColumns(List<TrinoExportColumnHandle> joinColumns) {
        return new TrinoExportTableHandle(schemaTableName, projectedColumns, predicateClause, limit, sortOrder, aggregation, fromClause, nextTableAlias, Optional.of(joinColumns), assignments);
    }

    public TrinoExportTableHandle withAssignments(java.util.Map<String, TrinoExportColumnHandle> assignments) {
        return new TrinoExportTableHandle(schemaTableName, projectedColumns, predicateClause, limit, sortOrder, aggregation, fromClause, nextTableAlias, joinColumns, Optional.of(assignments));
    }

    @JsonProperty
    public Optional<String> getFromClause() {
        return fromClause;
    }

    @JsonProperty
    public int getNextTableAlias() {
        return nextTableAlias;
    }

    /**
     * Check if this is a Top-N query (both ORDER BY and LIMIT are present)
     */
    public boolean isTopN() {
        return sortOrder.isPresent() && !sortOrder.get().isEmpty() && limit.isPresent();
    }

    /**
     * Check if this query has aggregation pushdown
     */
    public boolean hasAggregation() {
        return aggregation.isPresent();
    }

    /**
     * Check if this is a joined query
     */
    public boolean hasJoin() {
        return fromClause.isPresent();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TrinoExportTableHandle that = (TrinoExportTableHandle) o;
        return nextTableAlias == that.nextTableAlias &&
               Objects.equals(schemaTableName, that.schemaTableName) &&
               Objects.equals(projectedColumns, that.projectedColumns) &&
               Objects.equals(predicateClause, that.predicateClause) &&
               Objects.equals(limit, that.limit) &&
               Objects.equals(sortOrder, that.sortOrder) &&
               Objects.equals(aggregation, that.aggregation) &&
               Objects.equals(fromClause, that.fromClause) &&
               Objects.equals(joinColumns, that.joinColumns) &&
               Objects.equals(assignments, that.assignments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaTableName, projectedColumns, predicateClause, limit, sortOrder, aggregation, fromClause, nextTableAlias, joinColumns, assignments);
    }

    @Override
    public String toString() {
        String cols = projectedColumns.map(c -> c.toString()).orElse("*");
        String where = predicateClause.map(p -> " WHERE " + p).orElse("");
        String orderBy = sortOrder.map(SortItem::toSqlString).orElse("");
        String limitStr = limit.isPresent() ? " LIMIT " + limit.getAsLong() : "";
        String aggStr = aggregation.map(a -> " [AGG: " + a.toString() + "]").orElse("");
        String joinStr = fromClause.map(f -> " [JOIN: " + f + "]").orElse("");
        return schemaTableName.toString() + "[" + cols + "]" + where + orderBy + limitStr + aggStr + joinStr;
    }

    /**
     * Represents a single column in an ORDER BY clause
     */
    public static class SortItem {
        private final String columnName;
        private final boolean ascending;
        private final boolean nullsFirst;

        @JsonCreator
        public SortItem(
                @JsonProperty("columnName") String columnName,
                @JsonProperty("ascending") boolean ascending,
                @JsonProperty("nullsFirst") boolean nullsFirst) {
            this.columnName = columnName;
            this.ascending = ascending;
            this.nullsFirst = nullsFirst;
        }

        @JsonProperty
        public String getColumnName() {
            return columnName;
        }

        @JsonProperty
        public boolean isAscending() {
            return ascending;
        }

        @JsonProperty
        public boolean isNullsFirst() {
            return nullsFirst;
        }

        @Override
        public String toString() {
            return columnName + (ascending ? " ASC" : " DESC") + (nullsFirst ? " NULLS FIRST" : " NULLS LAST");
        }

        public static String toSqlString(List<SortItem> items) {
            if (items == null || items.isEmpty()) {
                return "";
            }
            StringBuilder sb = new StringBuilder();
            sb.append(" ORDER BY ");
            for (int i = 0; i < items.size(); i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(items.get(i).toString());
            }
            return sb.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SortItem sortItem = (SortItem) o;
            return ascending == sortItem.ascending &&
                   nullsFirst == sortItem.nullsFirst &&
                   Objects.equals(columnName, sortItem.columnName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(columnName, ascending, nullsFirst);
        }
    }

    /**
     * Stores aggregation pushdown information.
     * Contains the list of aggregate expressions and grouping columns.
     */
    public static class AggregationInfo {
        private final List<AggregateExpression> aggregates;
        private final List<String> groupByColumns;

        @JsonCreator
        public AggregationInfo(
                @JsonProperty("aggregates") List<AggregateExpression> aggregates,
                @JsonProperty("groupByColumns") List<String> groupByColumns) {
            this.aggregates = aggregates;
            this.groupByColumns = groupByColumns;
        }

        @JsonProperty
        public List<AggregateExpression> getAggregates() {
            return aggregates;
        }

        @JsonProperty
        public List<String> getGroupByColumns() {
            return groupByColumns;
        }

        @Override
        public String toString() {
            return "AggregationInfo{" +
                    "aggregates=" + aggregates +
                    ", groupByColumns=" + groupByColumns +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AggregationInfo that = (AggregationInfo) o;
            return Objects.equals(aggregates, that.aggregates) &&
                   Objects.equals(groupByColumns, that.groupByColumns);
        }

        @Override
        public int hashCode() {
            return Objects.hash(aggregates, groupByColumns);
        }
    }

    /**
     * Represents a single aggregate expression like COUNT(*), SUM(amount), etc.
     */
    public static class AggregateExpression {
        private final String functionName;      // COUNT, SUM, MIN, MAX, AVG
        private final String columnName;        // Column to aggregate, or "*" for COUNT(*)
        private final String outputAlias;       // Alias for the result column
        private final boolean distinct;         // For COUNT(DISTINCT col)

        @JsonCreator
        public AggregateExpression(
                @JsonProperty("functionName") String functionName,
                @JsonProperty("columnName") String columnName,
                @JsonProperty("outputAlias") String outputAlias,
                @JsonProperty("distinct") boolean distinct) {
            this.functionName = functionName;
            this.columnName = columnName;
            this.outputAlias = outputAlias;
            this.distinct = distinct;
        }

        @JsonProperty
        public String getFunctionName() {
            return functionName;
        }

        @JsonProperty
        public String getColumnName() {
            return columnName;
        }

        @JsonProperty
        public String getOutputAlias() {
            return outputAlias;
        }

        @JsonProperty
        public boolean isDistinct() {
            return distinct;
        }

        @Override
        public String toString() {
            return "AggregateExpression{" +
                    "functionName='" + functionName + '\'' +
                    ", columnName='" + columnName + '\'' +
                    ", outputAlias='" + outputAlias + '\'' +
                    ", distinct=" + distinct +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AggregateExpression that = (AggregateExpression) o;
            return distinct == that.distinct &&
                   Objects.equals(functionName, that.functionName) &&
                   Objects.equals(columnName, that.columnName) &&
                   Objects.equals(outputAlias, that.outputAlias);
        }

        @Override
        public int hashCode() {
            return Objects.hash(functionName, columnName, outputAlias, distinct);
        }
    }
}
