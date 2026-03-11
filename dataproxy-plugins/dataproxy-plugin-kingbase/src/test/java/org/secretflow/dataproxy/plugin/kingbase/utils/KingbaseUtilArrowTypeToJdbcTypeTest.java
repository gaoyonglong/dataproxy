package org.secretflow.dataproxy.plugin.kingbase.utils;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 *
 * @author chenmingliang
 * @date 2026/01/30
 */
public class KingbaseUtilArrowTypeToJdbcTypeTest {

    @Test
    public void testArrowTypeToJdbcType_Utf8() {
        ArrowType utf8Type = ArrowType.Utf8.INSTANCE;
        String jdbcType = KingbaseUtil.arrowTypeToJdbcType(utf8Type);
        assertEquals("VARCHAR", jdbcType);
    }

    @Test
    public void testArrowTypeToJdbcType_Int8Signed() {
        ArrowType int8Type = new ArrowType.Int(8, true);
        String jdbcType = KingbaseUtil.arrowTypeToJdbcType(int8Type);
        assertEquals("SMALLINT", jdbcType);
    }

    @Test
    public void testArrowTypeToJdbcType_Int8Unsigned() {
        ArrowType int8Type = new ArrowType.Int(8, false);
        String jdbcType = KingbaseUtil.arrowTypeToJdbcType(int8Type);
        assertEquals("SMALLINT", jdbcType);
    }

    @Test
    public void testArrowTypeToJdbcType_Int16Signed() {
        ArrowType int16Type = new ArrowType.Int(16, true);
        String jdbcType = KingbaseUtil.arrowTypeToJdbcType(int16Type);
        assertEquals("SMALLINT", jdbcType);
    }

    @Test
    public void testArrowTypeToJdbcType_Int16Unsigned() {
        ArrowType int16Type = new ArrowType.Int(16, false);
        String jdbcType = KingbaseUtil.arrowTypeToJdbcType(int16Type);
        assertEquals("SMALLINT", jdbcType);
    }

    @Test
    public void testArrowTypeToJdbcType_Int32Signed() {
        ArrowType int32Type = new ArrowType.Int(32, true);
        String jdbcType = KingbaseUtil.arrowTypeToJdbcType(int32Type);
        assertEquals("INTEGER", jdbcType);
    }

    @Test
    public void testArrowTypeToJdbcType_Int32Unsigned() {
        ArrowType int32Type = new ArrowType.Int(32, false);
        String jdbcType = KingbaseUtil.arrowTypeToJdbcType(int32Type);
        assertEquals("INTEGER", jdbcType);
    }

    @Test
    public void testArrowTypeToJdbcType_Int64Signed() {
        ArrowType int64Type = new ArrowType.Int(64, true);
        String jdbcType = KingbaseUtil.arrowTypeToJdbcType(int64Type);
        assertEquals("BIGINT", jdbcType);
    }

    @Test
    public void testArrowTypeToJdbcType_Int64Unsigned() {
        ArrowType int64Type = new ArrowType.Int(64, false);
        String jdbcType = KingbaseUtil.arrowTypeToJdbcType(int64Type);
        assertEquals("BIGINT", jdbcType);
    }

    @Test
    public void testArrowTypeToJdbcType_IntUnsupportedBitWidth() {
        ArrowType int128Type = new ArrowType.Int(128, true);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            KingbaseUtil.arrowTypeToJdbcType(int128Type);
        });
        assertTrue(exception.getMessage().contains("Unsupported Int bitWidth"));
    }

    @Test
    public void testArrowTypeToJdbcType_FloatSingle() {
        ArrowType floatType = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
        String jdbcType = KingbaseUtil.arrowTypeToJdbcType(floatType);
        assertEquals("REAL", jdbcType);
    }

    @Test
    public void testArrowTypeToJdbcType_FloatDouble() {
        ArrowType floatType = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        String jdbcType = KingbaseUtil.arrowTypeToJdbcType(floatType);
        assertEquals("DOUBLE PRECISION", jdbcType);
    }

    @Test
    public void testArrowTypeToJdbcType_FloatHalf() {
        ArrowType floatType = new ArrowType.FloatingPoint(FloatingPointPrecision.HALF);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            KingbaseUtil.arrowTypeToJdbcType(floatType);
        });
        assertTrue(exception.getMessage().contains("Unsupported floating point type"));
    }

    @Test
    public void testArrowTypeToJdbcType_Bool() {
        ArrowType boolType = ArrowType.Bool.INSTANCE;
        String jdbcType = KingbaseUtil.arrowTypeToJdbcType(boolType);
        assertEquals("BOOLEAN", jdbcType);
    }

    @Test
    public void testArrowTypeToJdbcType_Date() {
        ArrowType dateType = new ArrowType.Date(DateUnit.DAY);
        String jdbcType = KingbaseUtil.arrowTypeToJdbcType(dateType);
        assertEquals("DATE", jdbcType);
    }

    @Test
    public void testArrowTypeToJdbcType_Time() {
        ArrowType timeType = new ArrowType.Time(TimeUnit.MILLISECOND, 32);
        String jdbcType = KingbaseUtil.arrowTypeToJdbcType(timeType);
        assertEquals("TIME", jdbcType);
    }

    @Test
    public void testArrowTypeToJdbcType_Timestamp() {
        ArrowType timestampType = new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
        String jdbcType = KingbaseUtil.arrowTypeToJdbcType(timestampType);
        assertEquals("TIMESTAMP", jdbcType);
    }

    @Test
    public void testArrowTypeToJdbcType_TimestampWithTimezone() {
        ArrowType timestampType = new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC");
        String jdbcType = KingbaseUtil.arrowTypeToJdbcType(timestampType);
        assertEquals("TIMESTAMP", jdbcType);
    }

    @Test
    public void testArrowTypeToJdbcType_Decimal() {
        ArrowType decimalType = new ArrowType.Decimal(10, 2, 128);
        String jdbcType = KingbaseUtil.arrowTypeToJdbcType(decimalType);
        assertEquals("DECIMAL(10, 2)", jdbcType);
    }

    @Test
    public void testArrowTypeToJdbcType_DecimalHighPrecision() {
        ArrowType decimalType = new ArrowType.Decimal(38, 10, 128);
        String jdbcType = KingbaseUtil.arrowTypeToJdbcType(decimalType);
        assertEquals("DECIMAL(38, 10)", jdbcType);
    }

    @Test
    public void testArrowTypeToJdbcType_DecimalMinPrecision() {
        ArrowType decimalType = new ArrowType.Decimal(1, 0, 128);
        String jdbcType = KingbaseUtil.arrowTypeToJdbcType(decimalType);
        assertEquals("DECIMAL(1, 0)", jdbcType);
    }

    @Test
    public void testArrowTypeToJdbcType_DecimalMoneyType() {
        ArrowType decimalType = new ArrowType.Decimal(19, 4, 128);
        String jdbcType = KingbaseUtil.arrowTypeToJdbcType(decimalType);
        assertEquals("DECIMAL(19, 4)", jdbcType);
    }

    @Test
    public void testArrowTypeToJdbcType_Binary() {
        ArrowType binaryType = ArrowType.Binary.INSTANCE;
        String jdbcType = KingbaseUtil.arrowTypeToJdbcType(binaryType);
        assertEquals("BYTEA", jdbcType);
    }

    @Test
    public void testArrowTypeToJdbcType_FixedSizeBinary() {
        ArrowType fixedSizeBinaryType = new ArrowType.FixedSizeBinary(16);
        String jdbcType = KingbaseUtil.arrowTypeToJdbcType(fixedSizeBinaryType);
        assertEquals("BYTEA", jdbcType);
    }

    @Test
    public void testArrowTypeToJdbcType_UnsupportedType() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            KingbaseUtil.arrowTypeToJdbcType(new ArrowType.Duration(TimeUnit.MILLISECOND));
        });
        assertTrue(exception.getMessage().contains("Unsupported Arrow type"));
    }

    @Test
    public void testArrowTypeToJdbcType_AllIntTypes() {
        assertEquals("SMALLINT", KingbaseUtil.arrowTypeToJdbcType(new ArrowType.Int(8, true)));
        assertEquals("SMALLINT", KingbaseUtil.arrowTypeToJdbcType(new ArrowType.Int(8, false)));
        
        assertEquals("SMALLINT", KingbaseUtil.arrowTypeToJdbcType(new ArrowType.Int(16, true)));
        assertEquals("SMALLINT", KingbaseUtil.arrowTypeToJdbcType(new ArrowType.Int(16, false)));
        
        assertEquals("INTEGER", KingbaseUtil.arrowTypeToJdbcType(new ArrowType.Int(32, true)));
        assertEquals("INTEGER", KingbaseUtil.arrowTypeToJdbcType(new ArrowType.Int(32, false)));
        
        assertEquals("BIGINT", KingbaseUtil.arrowTypeToJdbcType(new ArrowType.Int(64, true)));
        assertEquals("BIGINT", KingbaseUtil.arrowTypeToJdbcType(new ArrowType.Int(64, false)));
    }
}