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

import io.trino.spi.connector.ColumnHandle;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Encapsulates SQL generation context including alias management and column mappings.
 * 
 * This class centralizes the state needed for SQL generation, particularly for:
 * - Join alias resolution (t0, t1, t2...)
 * - Column name qualification
 * - Type information for expression rewriting
 */
public class TeradataSqlContext {
    private final TrinoExportTableHandle tableHandle;
    private final TeradataQueryBuilder queryBuilder;
    private Map<String, ColumnHandle> assignments;

    public TeradataSqlContext(TrinoExportTableHandle tableHandle, TeradataQueryBuilder queryBuilder) {
        this.tableHandle = tableHandle;
        this.queryBuilder = queryBuilder;
        this.assignments = new HashMap<>();
        
        // Initialize from table handle if available
        tableHandle.getAssignments().ifPresent(a -> {
            for (Map.Entry<String, TrinoExportColumnHandle> entry : a.entrySet()) {
                this.assignments.put(entry.getKey(), entry.getValue());
            }
        });
    }

    /**
     * Set or update assignments for variable-to-column mapping.
     */
    public void setAssignments(Map<String, ColumnHandle> assignments) {
        this.assignments = new HashMap<>(assignments);
    }

    /**
     * Get assignments as ColumnHandle map (for expression rewriter compatibility).
     */
    public Map<String, ColumnHandle> getAssignments() {
        return assignments;
    }

    /**
     * Resolve a variable name to its qualified SQL column name.
     * Handles alias prefix for join scenarios (e.g., t0."column_name").
     */
    public String resolveColumnName(String variableName) {
        ColumnHandle handle = assignments.get(variableName);
        if (handle instanceof TrinoExportColumnHandle columnHandle) {
            // For joined tables, always look up the aliased version from joinColumns
            if (tableHandle.hasJoin() && tableHandle.getJoinColumns().isPresent()) {
                for (TrinoExportColumnHandle jc : tableHandle.getJoinColumns().get()) {
                    if (jc.getName().equalsIgnoreCase(columnHandle.getName())) {
                        // Return the aliased column from joinColumns
                        return jc.getQualifiedName(queryBuilder);
                    }
                }
            }
            return queryBuilder.getQualifiedColumnName(tableHandle, columnHandle);
        }
        // Fallback: use variable name directly with quoting
        return queryBuilder.maybeQuote(variableName);
    }

    /**
     * Get the underlying table handle.
     */
    public TrinoExportTableHandle getTableHandle() {
        return tableHandle;
    }

    /**
     * Get the query builder for SQL generation utilities.
     */
    public TeradataQueryBuilder getQueryBuilder() {
        return queryBuilder;
    }

    /**
     * Check if this context represents a joined query.
     */
    public boolean hasJoin() {
        return tableHandle.hasJoin();
    }

    /**
     * Check if this context has aggregation pushdown.
     */
    public boolean hasAggregation() {
        return tableHandle.hasAggregation();
    }

    /**
     * Check if this context has a predicate clause.
     */
    public boolean hasPredicate() {
        return tableHandle.getPredicateClause().isPresent();
    }

    /**
     * Get the FROM clause if this is a joined query.
     */
    public Optional<String> getFromClause() {
        return tableHandle.getFromClause();
    }
}
