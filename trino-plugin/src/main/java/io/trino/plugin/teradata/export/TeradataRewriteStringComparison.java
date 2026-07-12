package io.trino.plugin.teradata.export;

import com.google.common.collect.ImmutableList;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.expression.ComparisonOperator;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.CharType;
import io.trino.spi.type.VarcharType;

import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.variable;
import static io.trino.spi.type.BooleanType.BOOLEAN;

/**
 * Rewrites string comparison expressions to Teradata SQL.
 * 
 * Teradata string comparisons are case-sensitive and use standard SQL operators.
 * This handles VARCHAR and CHAR types with =, <>, <, >, <=, >= operators.
 */
public class TeradataRewriteStringComparison implements ConnectorExpressionRule<Call, ParameterizedExpression> {
    
    private static final Capture<Variable> FIRST_ARGUMENT = newCapture();
    private static final Capture<Variable> SECOND_ARGUMENT = newCapture();

    private static final Pattern<Call> PATTERN = call()
            .with(type().equalTo(BOOLEAN))
            .with(functionName().matching(Stream.of(ComparisonOperator.values())
                    .filter(comparison -> comparison != ComparisonOperator.IDENTICAL)
                    .map(ComparisonOperator::getFunctionName)
                    .collect(toImmutableSet())
                    ::contains))
            .with(argumentCount().equalTo(2))
            .with(argument(0).matching(variable()
                    .with(type().matching(t -> t instanceof CharType || t instanceof VarcharType))
                    .capturedAs(FIRST_ARGUMENT)))
            .with(argument(1).matching(variable()
                    .with(type().matching(t -> t instanceof CharType || t instanceof VarcharType))
                    .capturedAs(SECOND_ARGUMENT)));

    @Override
    public Pattern<Call> getPattern() {
        return PATTERN;
    }

    @Override
    public Optional<ParameterizedExpression> rewrite(Call expression, Captures captures, RewriteContext<ParameterizedExpression> context) {
        ComparisonOperator comparison = ComparisonOperator.forFunctionName(expression.getFunctionName());
        Variable firstArgument = captures.get(FIRST_ARGUMENT);
        Variable secondArgument = captures.get(SECOND_ARGUMENT);

        // Teradata uses case-sensitive comparisons by default (like Trino)
        // So we can push down string comparisons directly

        return context.defaultRewrite(firstArgument).flatMap(first ->
                context.defaultRewrite(secondArgument).map(second ->
                        new ParameterizedExpression(
                                "(%s) %s (%s)".formatted(first.expression(), comparison.getOperator(), second.expression()),
                                ImmutableList.<QueryParameter>builder()
                                        .addAll(first.parameters())
                                        .addAll(second.parameters())
                                        .build())));
    }
}
