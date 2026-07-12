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
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.VarcharType;

import java.util.Optional;

import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.constant;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.variable;
import static io.trino.spi.type.BooleanType.BOOLEAN;

/**
 * Rewrites LIKE expressions to Teradata SQL.
 * 
 * Teradata LIKE uses standard SQL LIKE semantics with % and _ wildcards.
 */
public class TeradataRewriteLike implements ConnectorExpressionRule<Call, ParameterizedExpression> {
    
    private static final Capture<Variable> VALUE = newCapture();
    private static final Capture<Constant> PATTERN = newCapture();

    private static final FunctionName LIKE_FUNCTION = new FunctionName("$like");

    // Pattern for: $like(column, 'pattern')
    private static final Pattern<Call> LIKE_PATTERN = call()
            .with(type().equalTo(BOOLEAN))
            .with(functionName().equalTo(LIKE_FUNCTION))
            .with(argumentCount().equalTo(2))
            .with(argument(0).matching(variable().capturedAs(VALUE)))
            .with(argument(1).matching(constant().with(type().matching(t -> t instanceof VarcharType)).capturedAs(PATTERN)));

    @Override
    public Pattern<Call> getPattern() {
        return LIKE_PATTERN;
    }

    @Override
    public Optional<ParameterizedExpression> rewrite(Call expression, Captures captures, RewriteContext<ParameterizedExpression> context) {
        Variable value = captures.get(VALUE);
        Constant pattern = captures.get(PATTERN);

        Optional<ParameterizedExpression> rewrittenValue = context.defaultRewrite(value);
        if (rewrittenValue.isEmpty()) {
            return Optional.empty();
        }

        // Get the pattern string
        io.airlift.slice.Slice patternSlice = (io.airlift.slice.Slice) pattern.getValue();
        if (patternSlice == null) {
            return Optional.empty();
        }
        String patternStr = patternSlice.toStringUtf8();
        
        // Escape single quotes in the pattern
        String escapedPattern = patternStr.replace("'", "''");

        // Check if there's an escape character (3 arguments)
        if (expression.getArguments().size() == 3) {
            ConnectorExpression escapeExpr = expression.getArguments().get(2);
            if (escapeExpr instanceof Constant escapeConstant) {
                io.airlift.slice.Slice escapeSlice = (io.airlift.slice.Slice) escapeConstant.getValue();
                if (escapeSlice != null) {
                    String escapeStr = escapeSlice.toStringUtf8();
                    return Optional.of(new ParameterizedExpression(
                            String.format("(%s) LIKE '%s' ESCAPE '%s'", 
                                    rewrittenValue.get().expression(), escapedPattern, escapeStr),
                            ImmutableList.<QueryParameter>builder()
                                    .addAll(rewrittenValue.get().parameters())
                                    .build()));
                }
            }
        }

        return Optional.of(new ParameterizedExpression(
                String.format("(%s) LIKE '%s'", rewrittenValue.get().expression(), escapedPattern),
                ImmutableList.<QueryParameter>builder()
                        .addAll(rewrittenValue.get().parameters())
                        .build()));
    }
}
