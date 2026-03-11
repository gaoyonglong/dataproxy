package org.secretflow.dataproxy.plugin.kingbase.utils;

import org.junit.jupiter.api.Test;

/**
 *
 * @author chenmingliang
 * @date 2026/01/30
 */
public class KingbaseDbTypeToKusciaTypeUtilTest {

    @Test
    public void testConvertsmallint() {
        KingbaseDbTypeToKusciaTypeUtil.parseKusciaColumnType("smallint");
    }

    @Test
    public void testConvertinteger() {
        KingbaseDbTypeToKusciaTypeUtil.parseKusciaColumnType("integer");
    }

    @Test
    public void testConvertbigint() {
        KingbaseDbTypeToKusciaTypeUtil.parseKusciaColumnType("bigint");
    }

    @Test
    public void testConvertfloat() {
        KingbaseDbTypeToKusciaTypeUtil.parseKusciaColumnType("float4");
    }

    @Test
    public void testConvertnumeric() {
        KingbaseDbTypeToKusciaTypeUtil.parseKusciaColumnType("numeric");
    }

    @Test
    public void testConvertdate() {
        KingbaseDbTypeToKusciaTypeUtil.parseKusciaColumnType("date");
    }

    @Test
    public void testConverttimestamp() {
        KingbaseDbTypeToKusciaTypeUtil.parseKusciaColumnType("timestamp");
    }

    @Test
    public void testConvertdate64() {
        KingbaseDbTypeToKusciaTypeUtil.parseKusciaColumnType("date64");
    }
    @Test
    public void testConvertdate32() {
        KingbaseDbTypeToKusciaTypeUtil.parseKusciaColumnType("date32");
    }
    @Test
    public void testConvertfloat64() {
        KingbaseDbTypeToKusciaTypeUtil.parseKusciaColumnType("float64");
    }
    @Test
    public void testConvertfloat32() {
        KingbaseDbTypeToKusciaTypeUtil.parseKusciaColumnType("float32");
    }
    @Test
    public void testConvertuint64() {
        KingbaseDbTypeToKusciaTypeUtil.parseKusciaColumnType("uint64");
    }
    @Test
    public void testConvertuint32() {
        KingbaseDbTypeToKusciaTypeUtil.parseKusciaColumnType("uint32");
    }
    @Test
    public void testConvertuint16() {
        KingbaseDbTypeToKusciaTypeUtil.parseKusciaColumnType("uint16");
    }
    @Test
    public void testConvertunit8() {
        KingbaseDbTypeToKusciaTypeUtil.parseKusciaColumnType("unit8");
    }
    @Test
    public void testConvertint64() {
        KingbaseDbTypeToKusciaTypeUtil.parseKusciaColumnType("int64");
    }
    @Test
    public void testConvertint32() {
        KingbaseDbTypeToKusciaTypeUtil.parseKusciaColumnType("int32");
    }
    @Test
    public void testConvertint16() {
        KingbaseDbTypeToKusciaTypeUtil.parseKusciaColumnType("int16");
    }
    @Test
    public void testConvertint8() {
        KingbaseDbTypeToKusciaTypeUtil.parseKusciaColumnType("int8");
    }
    @Test
    public void testConvertbytea() {
        KingbaseDbTypeToKusciaTypeUtil.parseKusciaColumnType("bytea");
    }
    @Test
    public void testConvertvarchar() {
        KingbaseDbTypeToKusciaTypeUtil.parseKusciaColumnType("varchar");
    }
    @Test
    public void testConvertboolean() {
        KingbaseDbTypeToKusciaTypeUtil.parseKusciaColumnType("boolean");
    }
    @Test
    public void testConverttime() {
        KingbaseDbTypeToKusciaTypeUtil.parseKusciaColumnType("time");
    }

    @Test
    public void testConvertbinary() {
        KingbaseDbTypeToKusciaTypeUtil.parseKusciaColumnType("binary");
    }

    @Test
    public void testConvertlarge_utf8() {
        KingbaseDbTypeToKusciaTypeUtil.parseKusciaColumnType("large_utf8");
    }

    @Test
    public void testConvertstring() {
        KingbaseDbTypeToKusciaTypeUtil.parseKusciaColumnType("string");
    }

    @Test
    public void testConvertbool() {
        KingbaseDbTypeToKusciaTypeUtil.parseKusciaColumnType("bool");
    }

}
