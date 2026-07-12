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
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.jdbc.expression.JdbcConnectorExpressionRewriterBuilder;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Unified expression rewriter for Teradata SQL generation.
 * 
 * Combines:
 * 1. Pattern-based rules via JdbcConnectorExpressionRewriterBuilder (like Oracle)
 * 2. Existing rewrite classes (TeradataRewriteCast, TeradataRewriteLike, TeradataRewriteStringComparison)
 * 3. YAML-based function mappings via FunctionMappingConfig
 */
public class TeradataExpressionRewriter {
    private static final Logger log = Logger.get(TeradataExpressionRewriter.class);
    
    private final ConnectorExpressionRewriter<ParameterizedExpression> patternRewriter;
    private final FunctionMappingConfig functionConfig;
    private final TeradataQueryBuilder queryBuilder;

    public TeradataExpressionRewriter(TeradataQueryBuilder queryBuilder, FunctionMappingConfig functionConfig) {
        this.queryBuilder = queryBuilder;
        this.functionConfig = functionConfig;
        
        this.patternRewriter = JdbcConnectorExpressionRewriterBuilder.newBuilder()
                .addStandardRules(queryBuilder::maybeQuote)
                // Numeric type class for comparison operators
                .withTypeClass("numeric_type", ImmutableSet.of(
                        "tinyint", "smallint", "integer", "bigint", "decimal", "real", "double"))
                // Numeric comparisons
                .map("$equal(left: numeric_type, right: numeric_type)").to("left = right")
                .map("$not_equal(left: numeric_type, right: numeric_type)").to("left <> right")
                .map("$less_than(left: numeric_type, right: numeric_type)").to("left < right")
                .map("$less_than_or_equal(left: numeric_type, right: numeric_type)").to("left <= right")
                .map("$greater_than(left: numeric_type, right: numeric_type)").to("left > right")
                .map("$greater_than_or_equal(left: numeric_type, right: numeric_type)").to("left >= right")
                // Add existing rewrite classes
                .add(new TeradataRewriteStringComparison())
                .add(new TeradataRewriteLike())
                .add(new TeradataRewriteCast())
                .build();
    }

    /**
     * Rewrite a ConnectorExpression to Teradata SQL.
     * 
     * @param context SQL generation context with alias and assignment info
     * @param expression The expression to rewrite
     * @return SQL string if expression can be rewritten, empty otherwise
     */
    public Optional<String> rewrite(TeradataSqlContext context, ConnectorExpression expression) {
        if (expression == null) {
            return Optional.empty();
        }

        // Note: Pattern-based rewriter requires non-null ConnectorSession, 
        // which we don't have in this context. Use manual handling instead.
        // The manual handling covers all operators we need for Teradata.
        return rewriteManually(context, expression);
    }

    /**
     * Manual rewriting for expressions not handled by pattern rewriter.
     * This covers boolean logic, NULL checks, and YAML-based function mappings.
     */
    private Optional<String> rewriteManually(TeradataSqlContext context, ConnectorExpression expression) {
        if (expression instanceof Constant constant) {
            return formatConstant(constant);
        }
        
        if (expression instanceof Variable variable) {
            return Optional.of(context.resolveColumnName(variable.getName()));
        }
        
        if (expression instanceof Call call) {
            return handleCall(context, call);
        }
        
        return Optional.empty();
    }

    private Optional<String> handleCall(TeradataSqlContext context, Call call) {
        String name = call.getFunctionName().getName();
        
        return switch (name) {
            // Boolean logic (multi-argument support)
            case "$and" -> handleBooleanAnd(context, call);
            case "$or" -> handleBooleanOr(context, call);
            case "$not" -> handleBooleanNot(context, call);
            
            // NULL checks
            case "$is_null" -> handleIsNull(context, call);
            
            // Comparison operators
            case "$equal" -> handleBinaryComparison(context, call, "=");
            case "$not_equal" -> handleBinaryComparison(context, call, "<>");
            case "$less_than" -> handleBinaryComparison(context, call, "<");
            case "$less_than_or_equal" -> handleBinaryComparison(context, call, "<=");
            case "$greater_than" -> handleBinaryComparison(context, call, ">");
            case "$greater_than_or_equal" -> handleBinaryComparison(context, call, ">=");
            case "$identical" -> handleBinaryComparison(context, call, "=");
            
            // IN list
            case "$in" -> handleInList(context, call);
            
            // BETWEEN
            case "$between" -> handleBetween(context, call);
            
            // Arithmetic
            case "$add" -> handleArithmetic(context, call, "+");
            case "$subtract" -> handleArithmetic(context, call, "-");
            case "$multiply" -> handleArithmetic(context, call, "*");
            case "$divide" -> handleArithmetic(context, call, "/");
            case "$negate" -> handleNegate(context, call);
            
            // LIKE pattern matching
            case "$like" -> handleLike(context, call);
            
            // CAST type conversion
            case "$cast" -> handleCast(context, call);
            
            // Try YAML-based function mappings for other functions
            default -> handleYamlMapping(context, call, name);
        };
    }

    private Optional<String> handleBooleanAnd(TeradataSqlContext context, Call call) {
        List<String> parts = new ArrayList<>();
        for (ConnectorExpression arg : call.getArguments()) {
            Optional<String> sql = rewrite(context, arg);
            if (sql.isEmpty()) return Optional.empty();
            parts.add(sql.get());
        }
        if (parts.size() == 1) return Optional.of(parts.get(0));
        return Optional.of("(" + String.join(" AND ", parts) + ")");
    }

    private Optional<String> handleBooleanOr(TeradataSqlContext context, Call call) {
        List<String> parts = new ArrayList<>();
        for (ConnectorExpression arg : call.getArguments()) {
            Optional<String> sql = rewrite(context, arg);
            if (sql.isEmpty()) return Optional.empty();
            parts.add(sql.get());
        }
        if (parts.size() == 1) return Optional.of(parts.get(0));
        return Optional.of("(" + String.join(" OR ", parts) + ")");
    }

    private Optional<String> handleBooleanNot(TeradataSqlContext context, Call call) {
        if (call.getArguments().size() != 1) return Optional.empty();
        
        // Special case: NOT(IS NULL) -> IS NOT NULL
        ConnectorExpression inner = call.getArguments().get(0);
        if (inner instanceof Call innerCall && "$is_null".equals(innerCall.getFunctionName().getName())) {
            if (innerCall.getArguments().size() == 1) {
                Optional<String> operand = rewrite(context, innerCall.getArguments().get(0));
                if (operand.isPresent()) {
                    return Optional.of("(" + operand.get() + " IS NOT NULL)");
                }
            }
        }
        
        Optional<String> operand = rewrite(context, inner);
        if (operand.isEmpty()) return Optional.empty();
        return Optional.of("(NOT " + operand.get() + ")");
    }

    private Optional<String> handleIsNull(TeradataSqlContext context, Call call) {
        if (call.getArguments().size() != 1) return Optional.empty();
        Optional<String> operand = rewrite(context, call.getArguments().get(0));
        if (operand.isEmpty()) return Optional.empty();
        return Optional.of("(" + operand.get() + " IS NULL)");
    }

    private Optional<String> handleBinaryComparison(TeradataSqlContext context, Call call, String operator) {
        if (call.getArguments().size() != 2) return Optional.empty();
        Optional<String> left = rewrite(context, call.getArguments().get(0));
        Optional<String> right = rewrite(context, call.getArguments().get(1));
        if (left.isEmpty() || right.isEmpty()) return Optional.empty();
        return Optional.of("(" + left.get() + " " + operator + " " + right.get() + ")");
    }

    private Optional<String> handleInList(TeradataSqlContext context, Call call) {
        if (call.getArguments().size() < 2) return Optional.empty();
        Optional<String> value = rewrite(context, call.getArguments().get(0));
        if (value.isEmpty()) return Optional.empty();
        
        List<String> inValues = new ArrayList<>();
        for (int i = 1; i < call.getArguments().size(); i++) {
            Optional<String> v = rewrite(context, call.getArguments().get(i));
            if (v.isEmpty()) return Optional.empty();
            inValues.add(v.get());
        }
        return Optional.of("(" + value.get() + " IN (" + String.join(", ", inValues) + "))");
    }

    private Optional<String> handleBetween(TeradataSqlContext context, Call call) {
        if (call.getArguments().size() != 3) return Optional.empty();
        Optional<String> value = rewrite(context, call.getArguments().get(0));
        Optional<String> low = rewrite(context, call.getArguments().get(1));
        Optional<String> high = rewrite(context, call.getArguments().get(2));
        if (value.isEmpty() || low.isEmpty() || high.isEmpty()) return Optional.empty();
        return Optional.of("(" + value.get() + " BETWEEN " + low.get() + " AND " + high.get() + ")");
    }

    private Optional<String> handleArithmetic(TeradataSqlContext context, Call call, String operator) {
        if (call.getArguments().size() != 2) return Optional.empty();
        Optional<String> left = rewrite(context, call.getArguments().get(0));
        Optional<String> right = rewrite(context, call.getArguments().get(1));
        if (left.isEmpty() || right.isEmpty()) return Optional.empty();
        return Optional.of("(" + left.get() + " " + operator + " " + right.get() + ")");
    }

    private Optional<String> handleNegate(TeradataSqlContext context, Call call) {
        if (call.getArguments().size() != 1) return Optional.empty();
        Optional<String> operand = rewrite(context, call.getArguments().get(0));
        if (operand.isEmpty()) return Optional.empty();
        return Optional.of("(-" + operand.get() + ")");
    }

    private Optional<String> handleLike(TeradataSqlContext context, Call call) {
        List<ConnectorExpression> args = call.getArguments();
        if (args.size() < 2) return Optional.empty();
        
        Optional<String> value = rewrite(context, args.get(0));
        if (value.isEmpty()) return Optional.empty();
        
        // Pattern must be a constant
        if (!(args.get(1) instanceof Constant patternConst)) return Optional.empty();
        Object patternValue = patternConst.getValue();
        if (patternValue == null) return Optional.empty();
        
        String pattern;
        if (patternValue instanceof io.airlift.slice.Slice slice) {
            pattern = slice.toStringUtf8();
        } else if (patternValue instanceof String s) {
            pattern = s;
        } else {
            return Optional.empty();
        }
        
        // Escape single quotes in pattern
        String escapedPattern = pattern.replace("'", "''");
        
        // Handle optional escape character
        if (args.size() == 3 && args.get(2) instanceof Constant escapeConst) {
            Object escapeValue = escapeConst.getValue();
            if (escapeValue != null) {
                String escape;
                if (escapeValue instanceof io.airlift.slice.Slice slice) {
                    escape = slice.toStringUtf8();
                } else if (escapeValue instanceof String s) {
                    escape = s;
                } else {
                    return Optional.empty();
                }
                return Optional.of(String.format("(%s LIKE '%s' ESCAPE '%s')", 
                        value.get(), escapedPattern, escape));
            }
        }
        
        return Optional.of(String.format("(%s LIKE '%s')", value.get(), escapedPattern));
    }

    private Optional<String> handleCast(TeradataSqlContext context, Call call) {
        if (call.getArguments().size() != 1) return Optional.empty();
        
        Optional<String> operand = rewrite(context, call.getArguments().get(0));
        if (operand.isEmpty()) return Optional.empty();
        
        // Get target type from the call's result type
        Type targetType = call.getType();
        String teradataType = mapTrinoTypeToTeradata(targetType);
        if (teradataType == null) return Optional.empty();
        
        return Optional.of(String.format("CAST(%s AS %s)", operand.get(), teradataType));
    }

    private String mapTrinoTypeToTeradata(Type type) {
        if (type instanceof io.trino.spi.type.VarcharType varcharType) {
            if (varcharType.isUnbounded()) {
                return "VARCHAR(64000)";
            }
            return "VARCHAR(" + varcharType.getBoundedLength() + ")";
        }
        if (type instanceof io.trino.spi.type.CharType charType) {
            return "CHAR(" + charType.getLength() + ")";
        }
        if (type instanceof io.trino.spi.type.DecimalType decimalType) {
            return "DECIMAL(" + decimalType.getPrecision() + "," + decimalType.getScale() + ")";
        }
        
        return switch (type.getBaseName()) {
            case "bigint" -> "BIGINT";
            case "integer" -> "INTEGER";
            case "smallint" -> "SMALLINT";
            case "tinyint" -> "BYTEINT";
            case "double" -> "FLOAT";
            case "real" -> "REAL";
            case "date" -> "DATE";
            case "timestamp" -> "TIMESTAMP";
            case "time" -> "TIME";
            case "boolean" -> "BYTEINT";
            default -> null;
        };
    }

    private Optional<String> handleYamlMapping(TeradataSqlContext context, Call call, String functionName) {
        String baseName = functionName.startsWith("$") ? functionName.substring(1) : functionName;
        
        if (functionConfig.isDisabled(baseName)) {
            log.debug("Function disabled for pushdown: %s", baseName);
            return Optional.empty();
        }
        
        Optional<String> template = functionConfig.getTemplate(baseName);
        if (template.isEmpty()) {
            log.debug("No mapping for function: %s", functionName);
            return Optional.empty();
        }
        
        List<String> args = new ArrayList<>();
        for (ConnectorExpression arg : call.getArguments()) {
            Optional<String> argSql = rewrite(context, arg);
            if (argSql.isEmpty()) return Optional.empty();
            args.add(argSql.get());
        }
        
        String result = FunctionMappingConfig.applyTemplate(template.get(), args);
        log.debug("Applied function template: %s -> %s", template.get(), result);
        return Optional.of(result);
    }

    private Optional<String> formatConstant(Constant constant) {
        if (constant.getValue() == null) {
            return Optional.of("NULL");
        }
        
        Type type = constant.getType();
        Object value = constant.getValue();
        
        String formatted = queryBuilder.formatValueForSql(value, type);
        return Optional.ofNullable(formatted);
    }
}
