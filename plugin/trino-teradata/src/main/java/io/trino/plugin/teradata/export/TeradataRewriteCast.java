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
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.sql.Types;
import java.util.Optional;

import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.variable;

/**
 * Rewrites CAST expressions to Teradata SQL.
 * 
 * Handles type conversions with Teradata-specific syntax:
 * - CAST(x AS VARCHAR(n))
 * - CAST(x AS INTEGER)
 * - CAST(x AS DECIMAL(p,s))
 */
public class TeradataRewriteCast implements ConnectorExpressionRule<Call, ParameterizedExpression> {
    
    private static final Capture<Variable> VALUE = newCapture();
    private static final FunctionName CAST_FUNCTION = new FunctionName("$cast");

    private static final Pattern<Call> CAST_PATTERN = call()
            .with(functionName().equalTo(CAST_FUNCTION))
            .with(argumentCount().equalTo(1))
            .with(argument(0).matching(variable().capturedAs(VALUE)));

    @Override
    public Pattern<Call> getPattern() {
        return CAST_PATTERN;
    }

    @Override
    public Optional<ParameterizedExpression> rewrite(Call expression, Captures captures, RewriteContext<ParameterizedExpression> context) {
        Variable value = captures.get(VALUE);
        Type targetType = expression.getType();

        Optional<ParameterizedExpression> rewrittenValue = context.defaultRewrite(value);
        if (rewrittenValue.isEmpty()) {
            return Optional.empty();
        }

        // Get source type from the variable
        Type sourceType = value.getType();

        String teradataType = mapToTeradataType(sourceType, targetType);
        if (teradataType == null) {
            return Optional.empty();
        }

        return Optional.of(new ParameterizedExpression(
                String.format("CAST(%s AS %s)", rewrittenValue.get().expression(), teradataType),
                ImmutableList.<QueryParameter>builder()
                        .addAll(rewrittenValue.get().parameters())
                        .build()));
    }

    /**
     * Map Trino types to Teradata SQL type syntax.
     */
    private static String mapToTeradataType(Type sourceType, Type targetType) {
        if (targetType instanceof VarcharType varcharType) {
            if (varcharType.isUnbounded()) {
                return "VARCHAR(64000)"; // Teradata max
            }
            return "VARCHAR(" + varcharType.getBoundedLength() + ")";
        }

        if (targetType instanceof CharType charType) {
            return "CHAR(" + charType.getLength() + ")";
        }

        if (targetType instanceof DecimalType decimalType) {
            return "DECIMAL(" + decimalType.getPrecision() + "," + decimalType.getScale() + ")";
        }

        return switch (targetType.getBaseName()) {
            case "bigint" -> "BIGINT";
            case "integer" -> "INTEGER";
            case "smallint" -> "SMALLINT";
            case "tinyint" -> "BYTEINT";
            case "double" -> "FLOAT";
            case "real" -> "REAL";
            case "date" -> "DATE";
            case "timestamp" -> "TIMESTAMP";
            case "time" -> "TIME";
            case "boolean" -> "BYTEINT"; // Teradata doesn't have boolean
            default -> null; // Unsupported
        };
    }

    /**
     * Determine if a CAST can be pushed down to Teradata.
     */
    public static boolean supportsPushdown(Type sourceType, Type targetType) {
        return mapToTeradataType(sourceType, targetType) != null;
    }

    /**
     * Create a JdbcTypeHandle for the target type.
     */
    public static Optional<JdbcTypeHandle> toJdbcTypeHandle(Type targetType) {
        if (targetType instanceof VarcharType varcharType) {
            int length = varcharType.isUnbounded() ? 64000 : varcharType.getBoundedLength();
            return Optional.of(new JdbcTypeHandle(
                    Types.VARCHAR, 
                    Optional.of("VARCHAR"), 
                    Optional.of(length), 
                    Optional.empty(), 
                    Optional.empty(), 
                    Optional.empty()));
        }

        if (targetType instanceof CharType charType) {
            return Optional.of(new JdbcTypeHandle(
                    Types.CHAR, 
                    Optional.of("CHAR"), 
                    Optional.of(charType.getLength()), 
                    Optional.empty(), 
                    Optional.empty(), 
                    Optional.empty()));
        }

        if (targetType instanceof DecimalType decimalType) {
            return Optional.of(new JdbcTypeHandle(
                    Types.DECIMAL, 
                    Optional.of("DECIMAL"), 
                    Optional.of(decimalType.getPrecision()), 
                    Optional.of(decimalType.getScale()), 
                    Optional.empty(), 
                    Optional.empty()));
        }

        return switch (targetType.getBaseName()) {
            case "bigint" -> Optional.of(new JdbcTypeHandle(Types.BIGINT, Optional.of("BIGINT"), Optional.of(19), Optional.of(0), Optional.empty(), Optional.empty()));
            case "integer" -> Optional.of(new JdbcTypeHandle(Types.INTEGER, Optional.of("INTEGER"), Optional.of(10), Optional.of(0), Optional.empty(), Optional.empty()));
            case "smallint" -> Optional.of(new JdbcTypeHandle(Types.SMALLINT, Optional.of("SMALLINT"), Optional.of(5), Optional.of(0), Optional.empty(), Optional.empty()));
            case "double" -> Optional.of(new JdbcTypeHandle(Types.DOUBLE, Optional.of("FLOAT"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
            default -> Optional.empty();
        };
    }
}
