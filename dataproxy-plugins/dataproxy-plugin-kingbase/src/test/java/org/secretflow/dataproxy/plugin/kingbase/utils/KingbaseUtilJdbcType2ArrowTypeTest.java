package org.secretflow.dataproxy.plugin.kingbase.utils;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * KingbaseUtil jdbcType2ArrowType 方法的单元测试
 *
 * @author chenmingliang
 * @date 2026/01/30
 */
public class KingbaseUtilJdbcType2ArrowTypeTest {

    @Test
    public void testJdbcType2ArrowType_NullType() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            KingbaseUtil.jdbcType2ArrowType(null);
        });
        assertEquals("Kingbase type is null or empty", exception.getMessage());
    }

    @Test
    public void testJdbcType2ArrowType_EmptyType() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            KingbaseUtil.jdbcType2ArrowType("");
        });
        assertEquals("Kingbase type is null or empty", exception.getMessage());
    }

    @Test
    public void testJdbcType2ArrowType_UppercaseTypeName() {
        ArrowType result = KingbaseUtil.jdbcType2ArrowType("INTEGER");
        assertTrue(result instanceof ArrowType.Int);
        assertEquals(32, ((ArrowType.Int) result).getBitWidth());
    }

    @Test
    public void testJdbcType2ArrowType_SmallInt() {
        ArrowType result = KingbaseUtil.jdbcType2ArrowType("smallint");
        assertTrue(result instanceof ArrowType.Int);
        assertEquals(16, ((ArrowType.Int) result).getBitWidth());
    }

    @Test
    public void testJdbcType2ArrowType_Int2() {
        ArrowType result = KingbaseUtil.jdbcType2ArrowType("int2");
        assertTrue(result instanceof ArrowType.Int);
        assertEquals(16, ((ArrowType.Int) result).getBitWidth());
    }

    @Test
    public void testJdbcType2ArrowType_Integer() {
        ArrowType result = KingbaseUtil.jdbcType2ArrowType("integer");
        assertTrue(result instanceof ArrowType.Int);
        assertEquals(32, ((ArrowType.Int) result).getBitWidth());
    }

    @Test
    public void testJdbcType2ArrowType_Int() {
        ArrowType result = KingbaseUtil.jdbcType2ArrowType("int");
        assertTrue(result instanceof ArrowType.Int);
        assertEquals(32, ((ArrowType.Int) result).getBitWidth());
    }

    @Test
    public void testJdbcType2ArrowType_Int4() {
        ArrowType result = KingbaseUtil.jdbcType2ArrowType("int4");
        assertTrue(result instanceof ArrowType.Int);
        assertEquals(32, ((ArrowType.Int) result).getBitWidth());
    }

    @Test
    public void testJdbcType2ArrowType_SmallSerial() {
        ArrowType result = KingbaseUtil.jdbcType2ArrowType("smallserial");
        assertTrue(result instanceof ArrowType.Int);
        assertEquals(32, ((ArrowType.Int) result).getBitWidth());
    }

    @Test
    public void testJdbcType2ArrowType_Serial2() {
        ArrowType result = KingbaseUtil.jdbcType2ArrowType("serial2");
        assertTrue(result instanceof ArrowType.Int);
        assertEquals(32, ((ArrowType.Int) result).getBitWidth());
    }

    @Test
    public void testJdbcType2ArrowType_Serial() {
        ArrowType result = KingbaseUtil.jdbcType2ArrowType("serial");
        assertTrue(result instanceof ArrowType.Int);
        assertEquals(32, ((ArrowType.Int) result).getBitWidth());
    }

    @Test
    public void testJdbcType2ArrowType_Serial4() {
        ArrowType result = KingbaseUtil.jdbcType2ArrowType("serial4");
        assertTrue(result instanceof ArrowType.Int);
        assertEquals(32, ((ArrowType.Int) result).getBitWidth());
    }

    @Test
    public void testJdbcType2ArrowType_BigSerial() {
        ArrowType result = KingbaseUtil.jdbcType2ArrowType("bigserial");
        assertTrue(result instanceof ArrowType.Int);
        assertEquals(32, ((ArrowType.Int) result).getBitWidth());
    }

    @Test
    public void testJdbcType2ArrowType_Serial8() {
        ArrowType result = KingbaseUtil.jdbcType2ArrowType("serial8");
        assertTrue(result instanceof ArrowType.Int);
        assertEquals(32, ((ArrowType.Int) result).getBitWidth());
    }

    @Test
    public void testJdbcType2ArrowType_BigInt() {
        ArrowType result = KingbaseUtil.jdbcType2ArrowType("bigint");
        assertTrue(result instanceof ArrowType.Int);
        assertEquals(64, ((ArrowType.Int) result).getBitWidth());
    }

    @Test
    public void testJdbcType2ArrowType_Int8() {
        ArrowType result = KingbaseUtil.jdbcType2ArrowType("int8");
        assertTrue(result instanceof ArrowType.Int);
        assertEquals(64, ((ArrowType.Int) result).getBitWidth());
    }

    @Test
    public void testJdbcType2ArrowType_Real() {
        ArrowType result = KingbaseUtil.jdbcType2ArrowType("real");
        assertTrue(result instanceof ArrowType.FloatingPoint);
        assertEquals(FloatingPointPrecision.SINGLE, ((ArrowType.FloatingPoint) result).getPrecision());
    }

    @Test
    public void testJdbcType2ArrowType_Float4() {
        ArrowType result = KingbaseUtil.jdbcType2ArrowType("float4");
        assertTrue(result instanceof ArrowType.FloatingPoint);
        assertEquals(FloatingPointPrecision.SINGLE, ((ArrowType.FloatingPoint) result).getPrecision());
    }

    @Test
    public void testJdbcType2ArrowType_DoublePrecision() {
        ArrowType result = KingbaseUtil.jdbcType2ArrowType("double precision");
        assertTrue(result instanceof ArrowType.FloatingPoint);
        assertEquals(FloatingPointPrecision.DOUBLE, ((ArrowType.FloatingPoint) result).getPrecision());
    }

    @Test
    public void testJdbcType2ArrowType_Float8() {
        ArrowType result = KingbaseUtil.jdbcType2ArrowType("float8");
        assertTrue(result instanceof ArrowType.FloatingPoint);
        assertEquals(FloatingPointPrecision.DOUBLE, ((ArrowType.FloatingPoint) result).getPrecision());
    }

    @Test
    public void testJdbcType2ArrowType_Numeric() {
        ArrowType result = KingbaseUtil.jdbcType2ArrowType("numeric");
        assertTrue(result instanceof ArrowType.FloatingPoint);
        assertEquals(FloatingPointPrecision.DOUBLE, ((ArrowType.FloatingPoint) result).getPrecision());
    }

    @Test
    public void testJdbcType2ArrowType_CharacterVarying() {
        ArrowType result = KingbaseUtil.jdbcType2ArrowType("character varying");
        assertTrue(result instanceof ArrowType.Utf8);
    }

    @Test
    public void testJdbcType2ArrowType_Varchar() {
        ArrowType result = KingbaseUtil.jdbcType2ArrowType("varchar");
        assertTrue(result instanceof ArrowType.Utf8);
    }

    @Test
    public void testJdbcType2ArrowType_Text() {
        ArrowType result = KingbaseUtil.jdbcType2ArrowType("text");
        assertTrue(result instanceof ArrowType.Utf8);
    }

    @Test
    public void testJdbcType2ArrowType_Char() {
        ArrowType result = KingbaseUtil.jdbcType2ArrowType("char");
        assertTrue(result instanceof ArrowType.Utf8);
    }

    @Test
    public void testJdbcType2ArrowType_Enum() {
        ArrowType result = KingbaseUtil.jdbcType2ArrowType("enum");
        assertTrue(result instanceof ArrowType.Utf8);
    }

    @Test
    public void testJdbcType2ArrowType_Json() {
        ArrowType result = KingbaseUtil.jdbcType2ArrowType("json");
        assertTrue(result instanceof ArrowType.Utf8);
    }

    @Test
    public void testJdbcType2ArrowType_Boolean() {
        ArrowType result = KingbaseUtil.jdbcType2ArrowType("boolean");
        assertTrue(result instanceof ArrowType.Bool);
    }

    @Test
    public void testJdbcType2ArrowType_Bool() {
        ArrowType result = KingbaseUtil.jdbcType2ArrowType("bool");
        assertTrue(result instanceof ArrowType.Bool);
    }

    @Test
    public void testJdbcType2ArrowType_Date() {
        ArrowType result = KingbaseUtil.jdbcType2ArrowType("date");
        assertTrue(result instanceof ArrowType.Date);
        assertEquals(DateUnit.DAY, ((ArrowType.Date) result).getUnit());
    }

    @Test
    public void testJdbcType2ArrowType_Timestamp() {
        ArrowType result = KingbaseUtil.jdbcType2ArrowType("timestamp");
        assertTrue(result instanceof ArrowType.Timestamp);
        assertEquals(TimeUnit.MILLISECOND, ((ArrowType.Timestamp) result).getUnit());
        assertNull(((ArrowType.Timestamp) result).getTimezone());
    }

    @Test
    public void testJdbcType2ArrowType_Timestamptz() {
        ArrowType result = KingbaseUtil.jdbcType2ArrowType("timestamptz");
        assertTrue(result instanceof ArrowType.Timestamp);
        assertEquals(TimeUnit.MILLISECOND, ((ArrowType.Timestamp) result).getUnit());
        assertEquals("UTC", ((ArrowType.Timestamp) result).getTimezone());
    }

    @Test
    public void testJdbcType2ArrowType_Time() {
        ArrowType result = KingbaseUtil.jdbcType2ArrowType("time");
        assertTrue(result instanceof ArrowType.Time);
        assertEquals(TimeUnit.MILLISECOND, ((ArrowType.Time) result).getUnit());
        assertEquals(32, ((ArrowType.Time) result).getBitWidth());
    }

    @Test
    public void testJdbcType2ArrowType_Bytea() {
        ArrowType result = KingbaseUtil.jdbcType2ArrowType("bytea");
        assertTrue(result instanceof ArrowType.Binary);
    }

    @Test
    public void testJdbcType2ArrowType_Oid() {
        ArrowType result = KingbaseUtil.jdbcType2ArrowType("oid");
        assertTrue(result instanceof ArrowType.Int);
        assertEquals(32, ((ArrowType.Int) result).getBitWidth());
    }

    @Test
    public void testJdbcType2ArrowType_DecimalWithPrecisionAndScale() {
        ArrowType result = KingbaseUtil.jdbcType2ArrowType("decimal(10,2)");
        assertTrue(result instanceof ArrowType.Decimal);
        ArrowType.Decimal decimalType = (ArrowType.Decimal) result;
        assertEquals(10, decimalType.getPrecision());
        assertEquals(2, decimalType.getScale());
    }

    @Test
    public void testJdbcType2ArrowType_DecimalHighPrecision() {
        ArrowType result = KingbaseUtil.jdbcType2ArrowType("decimal(38,10)");
        assertTrue(result instanceof ArrowType.Decimal);
        ArrowType.Decimal decimalType = (ArrowType.Decimal) result;
        assertEquals(38, decimalType.getPrecision());
        assertEquals(10, decimalType.getScale());
    }

    @Test
    public void testJdbcType2ArrowType_DecimalDefault() {
        ArrowType result = KingbaseUtil.jdbcType2ArrowType("decimal");
        assertTrue(result instanceof ArrowType.Decimal);
        ArrowType.Decimal decimalType = (ArrowType.Decimal) result;
        assertEquals(38, decimalType.getPrecision());
        assertEquals(10, decimalType.getScale());
    }

    @Test
    public void testJdbcType2ArrowType_DecimalVariousCombinations() {
        ArrowType result1 = KingbaseUtil.jdbcType2ArrowType("decimal(1,0)");
        assertEquals(1, ((ArrowType.Decimal) result1).getPrecision());
        assertEquals(0, ((ArrowType.Decimal) result1).getScale());

        ArrowType result2 = KingbaseUtil.jdbcType2ArrowType("decimal(20,5)");
        assertEquals(20, ((ArrowType.Decimal) result2).getPrecision());
        assertEquals(5, ((ArrowType.Decimal) result2).getScale());

        ArrowType result3 = KingbaseUtil.jdbcType2ArrowType("decimal(5,2)");
        assertEquals(5, ((ArrowType.Decimal) result3).getPrecision());
        assertEquals(2, ((ArrowType.Decimal) result3).getScale());
    }

    @Test
    public void testJdbcType2ArrowType_TypeWithWhitespace() {
        ArrowType result = KingbaseUtil.jdbcType2ArrowType("  int  ");
        assertTrue(result instanceof ArrowType.Int);
        assertEquals(32, ((ArrowType.Int) result).getBitWidth());
    }

    @Test
    public void testJdbcType2ArrowType_UnsupportedType() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            KingbaseUtil.jdbcType2ArrowType("unsupported_type");
        });
        assertTrue(exception.getMessage().contains("Unsupported Kingbase type"));
    }

    @Test
    public void testJdbcType2ArrowType_Clob() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            KingbaseUtil.jdbcType2ArrowType("clob");
        });
        assertTrue(exception.getMessage().contains("Unsupported Kingbase type"));
    }

    @Test
    public void testJdbcType2ArrowType_Blob() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            KingbaseUtil.jdbcType2ArrowType("blob");
        });
        assertTrue(exception.getMessage().contains("Unsupported Kingbase type"));
    }

    @Test
    public void testJdbcType2ArrowType_ArrayType() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            KingbaseUtil.jdbcType2ArrowType("integer[]");
        });
        assertTrue(exception.getMessage().contains("Unsupported Kingbase type"));
    }
}
