package io.trino.plugin.teradata.export;

import io.airlift.log.Logger;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Chars;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarbinaryType.VARBINARY;

/**
 * High-performance direct parser that converts Teradata binary format directly to Trino Pages.
 * 
 * Optimized Architecture: Single-Pass Row-Wise Parsing.
 * - Iterates the binary buffer exactly once (cache-friendly).
 * - Appends to BlockBuilders immediately.
 * - Zero intermediate allocations (no byte arrays per cell).
 * - Applies timezone correction for TIME and TIMESTAMP values.
 */
public final class DirectTrinoPageParser {
    private static final Logger log = Logger.get(DirectTrinoPageParser.class);
    
    // Timezone offset in seconds (derived from teradata.timezone config)
    // This is the offset of the Teradata server's timezone from UTC.
    // Positive values mean ahead of UTC (e.g., +08:00 = 28800 seconds)
    // Negative values mean behind UTC (e.g., -05:00 = -18000 seconds)
    private static volatile int teradataTimezoneOffsetSeconds = 0;
    
    /**
     * Set the Teradata timezone offset. Called during connector initialization.
     * @param offsetSeconds The offset in seconds (e.g., -18000 for -05:00)
     */
    public static void setTeradataTimezoneOffset(int offsetSeconds) {
        teradataTimezoneOffsetSeconds = offsetSeconds;
        log.info("DirectTrinoPageParser timezone offset set to %d seconds", offsetSeconds);
    }
    
    /**
     * Parse a timezone string (e.g., "-05:00" or "+08:00") to offset in seconds.
     * @param timezone The timezone string in format +/-HH:MM
     * @return The offset in seconds
     */
    public static int parseTimezoneToSeconds(String timezone) {
        if (timezone == null || timezone.isEmpty()) {
            return 0;
        }
        try {
            ZoneOffset offset = ZoneOffset.of(timezone);
            return offset.getTotalSeconds();
        } catch (Exception e) {
            log.warn("Failed to parse timezone '%s', using 0 offset", timezone);
            return 0;
        }
    }

    public record ColumnSpec(String name, String type, Type trinoType) {}

    /**
     * Parse Teradata binary batch directly to Trino Page.
     * This iterates the row-oriented buffer and builds columnar blocks on the fly.
     */
    public static Page parseDirectToPage(byte[] data, int length, List<ColumnSpec> columns) {
        ByteBuffer buf = ByteBuffer.wrap(data, 0, length);
        
        // Safety check for empty buffer
        if (buf.remaining() < 4) {
            return new Page(0);
        }

        int numRows = buf.getInt();
        if (numRows == 0) {
            return new Page(0);
        }

        int colCount = columns.size();
        BlockBuilder[] builders = new BlockBuilder[colCount];

        // Initialize BlockBuilders
        for (int i = 0; i < colCount; i++) {
            builders[i] = columns.get(i).trinoType.createBlockBuilder(null, numRows);
        }

        // Single Pass: Iterate Rows -> Columns
        try {
            for (int row = 0; row < numRows; row++) {
                for (int col = 0; col < colCount; col++) {
                    byte isNull = buf.get();
                    if (isNull == 1) {
                        builders[col].appendNull();
                    } else {
                        readAndAppendValue(buf, builders[col], columns.get(col));
                    }
                }
            }
        } catch (Exception e) {
            log.error(e, "Error parsing binary batch. Offset: %d, Length: %d", buf.position(), length);
            throw new RuntimeException("Error parsing Teradata binary batch", e);
        }

        // Build final Blocks
        Block[] blocks = new Block[colCount];
        for (int i = 0; i < colCount; i++) {
            blocks[i] = builders[i].build();
        }

        return new Page(numRows, blocks);
    }

    private static void readAndAppendValue(ByteBuffer buf, BlockBuilder builder, ColumnSpec spec) {
        String type = spec.type;
        Type trinoType = spec.trinoType;

        switch (type) {
            case "INTEGER", "DATE" -> {
                int val = buf.getInt();
                if (trinoType == INTEGER) {
                    INTEGER.writeLong(builder, val);
                } else {
                    // DATE is 4 bytes but stored as long in Trino
                    trinoType.writeLong(builder, val);
                }
            }
            case "BIGINT", "DECIMAL_SHORT" -> {
                long val = buf.getLong();
                // Check if Trino expects a long decimal (precision > 18)
                if (trinoType instanceof DecimalType decimalType && !decimalType.isShort()) {
                    // Convert 8-byte short decimal to 16-byte Int128 for long decimal types
                    decimalType.writeObject(builder, Int128.valueOf(val));
                } else {
                    trinoType.writeLong(builder, val);
                }
            }
            case "TIME" -> {
                long val = buf.getLong();
                // Apply timezone correction: TIME is in picoseconds since midnight
                // Teradata sends local time values, we need to adjust by the timezone offset
                // to get the correct representation when displayed
                // offset is in seconds, TIME is in picoseconds (10^12)
                long adjustedVal = val + ((long) teradataTimezoneOffsetSeconds * 1_000_000_000_000L);
                // Handle wrap-around for TIME (stays within 0-24 hours)
                long picosPerDay = 24L * 60L * 60L * 1_000_000_000_000L;
                if (adjustedVal < 0) {
                    adjustedVal += picosPerDay;
                } else if (adjustedVal >= picosPerDay) {
                    adjustedVal %= picosPerDay;
                }
                trinoType.writeLong(builder, adjustedVal);
            }
            case "TIMESTAMP" -> {
                long val = buf.getLong();
                // Apply timezone correction: TIMESTAMP is in microseconds since epoch
                // Teradata sends local time values, we need to adjust by the timezone offset
                // offset is in seconds, TIMESTAMP is in microseconds (10^6)
                long adjustedVal = val + ((long) teradataTimezoneOffsetSeconds * 1_000_000L);
                trinoType.writeLong(builder, adjustedVal);
            }
            case "DOUBLE" -> {
                double val = buf.getDouble();
                if (trinoType == DOUBLE) {
                    DOUBLE.writeDouble(builder, val);
                } else {
                     // Fallback check?
                     trinoType.writeDouble(builder, val);
                }
            }
            case "DECIMAL_LONG" -> {
                if (trinoType instanceof DecimalType decimalType) {
                    byte[] bytes = new byte[16];
                    buf.get(bytes);
                    decimalType.writeObject(builder, Int128.fromBigEndian(bytes));
                } else {
                    buf.position(buf.position() + 16);
                    builder.appendNull(); 
                }
            }
            default -> {
                // VARCHAR / VARBINARY / fallback length-prefixed payload
                int len = buf.getShort() & 0xFFFF;
                if (len == 0) {
                    trinoType.writeSlice(builder, Slices.EMPTY_SLICE);
                    return;
                }

                if (buf.hasArray()) {
                    // ZERO-ALLOCATION: Use Slice view of the underlying buffer array
                    io.airlift.slice.Slice slice = io.airlift.slice.Slices.wrappedBuffer(buf.array(), buf.arrayOffset() + buf.position(), len);
                    if (trinoType instanceof CharType) {
                        slice = Chars.trimTrailingSpaces(slice);
                    }
                    trinoType.writeSlice(builder, slice);
                    buf.position(buf.position() + len);
                } else {
                    byte[] strBytes = new byte[len];
                    buf.get(strBytes);
                    io.airlift.slice.Slice slice = io.airlift.slice.Slices.wrappedBuffer(strBytes);
                    if (trinoType instanceof CharType) {
                        slice = Chars.trimTrailingSpaces(slice);
                    }
                    trinoType.writeSlice(builder, slice);
                }
            }
        }
    }
}
