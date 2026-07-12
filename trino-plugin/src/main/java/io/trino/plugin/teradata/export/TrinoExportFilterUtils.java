package io.trino.plugin.teradata.export;

import io.airlift.slice.Slice;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public final class TrinoExportFilterUtils {
    private TrinoExportFilterUtils() {}

    public static String domainToSql(String columnName, Type type, Domain domain) {
        if (domain.isAll()) {
            return null;
        }
        if (domain.isNone()) {
            return "1=0";
        }

        ValueSet valueSet = domain.getValues();

        if (valueSet instanceof SortedRangeSet rangeSet) {
            // Equality
            if (rangeSet.getRangeCount() == 1) {
                Range range = rangeSet.getOrderedRanges().get(0);
                if (range.isSingleValue()) {
                    String value = formatValue(range.getSingleValue(), type);
                    if (value != null) {
                        if (domain.isNullAllowed()) {
                            return String.format("(%s = %s OR %s IS NULL)", columnName, value, columnName);
                        }
                        return String.format("%s = %s", columnName, value);
                    }
                }
            }

            // IN list
            if (rangeSet.getOrderedRanges().stream().allMatch(Range::isSingleValue)) {
                List<String> values = new ArrayList<>();
                for (Range range : rangeSet.getOrderedRanges()) {
                    String value = formatValue(range.getSingleValue(), type);
                    if (value != null) {
                        values.add(value);
                    } else {
                        return null;
                    }
                }
                if (!values.isEmpty()) {
                    String inList = String.join(", ", values);
                    if (domain.isNullAllowed()) {
                        return String.format("(%s IN (%s) OR %s IS NULL)", columnName, inList, columnName);
                    }
                    return String.format("%s IN (%s)", columnName, inList);
                }
            }

            // Range predicates (BETWEEN, <, >, <=, >=)
            List<String> rangeParts = new ArrayList<>();
            for (Range range : rangeSet.getOrderedRanges()) {
                String rangeSql = rangeToSql(columnName, type, range);
                if (rangeSql != null) {
                    rangeParts.add(rangeSql);
                } else {
                    // If any range can't be converted, give up on pushing this predicate
                    return null;
                }
            }
            if (!rangeParts.isEmpty()) {
                String combined = rangeParts.size() == 1 
                    ? rangeParts.get(0) 
                    : "(" + String.join(" OR ", rangeParts) + ")";
                if (domain.isNullAllowed()) {
                    return String.format("(%s OR %s IS NULL)", combined, columnName);
                }
                return combined;
            }
        }

        return null;
    }

    /**
     * Convert a Range to a SQL predicate like "col > 5 AND col < 10" or "col >= 100".
     */
    private static String rangeToSql(String columnName, Type type, Range range) {
        if (range.isSingleValue()) {
            String value = formatValue(range.getSingleValue(), type);
            return value != null ? String.format("%s = %s", columnName, value) : null;
        }

        if (range.isAll()) {
            return null; // No bounds, can't push down
        }

        List<String> parts = new ArrayList<>();

        // Low bound (col > X or col >= X)
        if (!range.isLowUnbounded()) {
            String lowValue = formatValue(range.getLowBoundedValue(), type);
            if (lowValue == null) return null;
            String op = range.isLowInclusive() ? ">=" : ">";
            parts.add(String.format("%s %s %s", columnName, op, lowValue));
        }

        // High bound (col < X or col <= X)
        if (!range.isHighUnbounded()) {
            String highValue = formatValue(range.getHighBoundedValue(), type);
            if (highValue == null) return null;
            String op = range.isHighInclusive() ? "<=" : "<";
            parts.add(String.format("%s %s %s", columnName, op, highValue));
        }

        if (parts.isEmpty()) {
            return null;
        }

        // Combine with AND for BETWEEN-like ranges
        return String.join(" AND ", parts);
    }

    private static String formatValue(Object value, Type type) {
        if (value == null) {
            return "NULL";
        }

        if (type instanceof VarcharType) {
            String strValue = ((Slice) value).toStringUtf8();
            return "'" + strValue.replace("'", "''") + "'";
        }

        if (type == IntegerType.INTEGER || type == BigintType.BIGINT ||
            type == SmallintType.SMALLINT || type == TinyintType.TINYINT) {
            return value.toString();
        }

        if (type == DoubleType.DOUBLE || type == RealType.REAL) {
            return value.toString();
        }

        if (type == BooleanType.BOOLEAN) {
            return ((Boolean) value) ? "1" : "0";
        }

        // DATE type: value is epoch days (long)
        if (type instanceof DateType) {
            long epochDays = (Long) value;
            LocalDate date = LocalDate.ofEpochDay(epochDays);
            return "DATE '" + date.toString() + "'";
        }

        // TIMESTAMP type: value is epoch microseconds (long)
        if (type instanceof TimestampType) {
            long epochMicros = (Long) value;
            long epochSeconds = epochMicros / 1_000_000;
            int nanoAdjustment = (int) ((epochMicros % 1_000_000) * 1000);
            LocalDateTime dt = LocalDateTime.ofEpochSecond(epochSeconds, nanoAdjustment, ZoneOffset.UTC);
            String ts = dt.toString().replace('T', ' ');
            // Ensure we have microseconds precision
            if (!ts.contains(".")) {
                ts += ".000000";
            } else {
                // Pad to 6 digits
                int dotIdx = ts.indexOf('.');
                int fracLen = ts.length() - dotIdx - 1;
                if (fracLen < 6) {
                    ts += "000000".substring(0, 6 - fracLen);
                }
            }
            return "TIMESTAMP '" + ts + "'";
        }

        return null;
    }
}
