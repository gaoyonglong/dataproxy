package org.secretflow.dataproxy.plugin.kingbase.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.secretflow.dataproxy.plugin.database.config.DatabaseConnectConfig;
import org.secretflow.dataproxy.plugin.database.writer.DatabaseRecordWriter;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author chenmingliang
 * @date 2025/12/10
 */
@Slf4j
public class KingbaseUtilsTest {


    @Test
    public void testInitKingbase() {

        String username = "kuscia";
        String password = "Oasis-119";
        String endpoint = "127.0.0.1:54321";
        String databaseName = "alice";

        DatabaseConnectConfig config = new DatabaseConnectConfig(username, password, endpoint, databaseName);

        try {

            KingbaseUtil.initKingbase(config);
        }catch (Exception e) {
            log.error("init kingbase test fail {}",e.getMessage());
        }
    }


    @Test
    public void testInitKingbaseIpErr() {

        String username = "kuscia";
        String password = "Oasis-119";
        String endpoint = "a127.0.0.1:54321";
        String databaseName = "alice";

        DatabaseConnectConfig config = new DatabaseConnectConfig(username, password, endpoint, databaseName);

        try {

            KingbaseUtil.initKingbase(config);
        }catch (Exception e) {
            log.error("init kingbase test fail {}",e.getMessage());
        }
    }


    @Test
    public void testInitKingbaseIpErrContainspoint() {

        String username = "kuscia";
        String password = "Oasis-119";
        String endpoint = "a127.0.0.1:54321..";
        String databaseName = "alice";

        DatabaseConnectConfig config = new DatabaseConnectConfig(username, password, endpoint, databaseName);

        try {

            KingbaseUtil.initKingbase(config);
        }catch (Exception e) {
            log.error("init kingbase test fail {}",e.getMessage());
        }
    }

    @Test
    public void testInitKingbaseIpErrContainspoint1() {

        String username = "kuscia";
        String password = "Oasis-119";
        String endpoint = "a127..0.0.1:54321";
        String databaseName = "alice";

        DatabaseConnectConfig config = new DatabaseConnectConfig(username, password, endpoint, databaseName);

        try {

            KingbaseUtil.initKingbase(config);
        }catch (Exception e) {
            log.error("init kingbase test fail {}",e.getMessage());
        }
    }

    @Test
    public void testInitKingbaseIpdatabaseErr() {

        String username = "kuscia";
        String password = "Oasis-119";
        String endpoint = "a127.0.0.1:54321";
        String databaseName = "ali-ce";

        DatabaseConnectConfig config = new DatabaseConnectConfig(username, password, endpoint, databaseName);

        try {

            KingbaseUtil.initKingbase(config);
        }catch (Exception e) {
            log.error("init kingbase test fail {}",e.getMessage());
        }
    }
    @Test
    public void testBuildQuerySql() {
        List<String> columns = Arrays.asList("column1", "column2", "column3");
        DatabaseRecordWriter.SqlWithParams sqlWithParams = KingbaseUtil.buildQuerySql("test_table", columns, null);

        assertEquals("SELECT column1, column2, column3 FROM test_table", sqlWithParams.sql);
        assertTrue(sqlWithParams.params.isEmpty());
    }

    @Test
    public void testBuildQuerySql_WhereCause() {
        org.secretflow.dataproxy.common.exceptions.DataproxyException exception = assertThrows(
            org.secretflow.dataproxy.common.exceptions.DataproxyException.class, () -> {
                List<String> columns = Arrays.asList("column1", "column2", "column3");
                KingbaseUtil.buildQuerySql("test_table", columns, "where column1 != null");
            }
        );
        assertEquals(org.secretflow.dataproxy.common.exceptions.DataproxyErrorCode.PARAMS_UNRELIABLE, exception.getErrorCode());
        assertNotNull(exception.getMessage());
    }

    @Test
    public void testCreateQuerySql() {
        Field column1 = new Field("column1", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field column2 = new Field("column2", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);

        List<Field> columns = Arrays.asList(column1, column2);

        Schema schema = new Schema(columns);

        String sql = KingbaseUtil.buildCreateTableSql("kingbasetablename", schema, null);

        assertTrue(sql.contains("CREATE TABLE kingbasetablename"));
        assertTrue(sql.contains("column1 INTEGER"));
        assertTrue(sql.contains("column2 VARCHAR"));
    }

    @Test
    public void testEscape() {
        KingbaseUtil.escapeString("test");
    }


    @Test
    void testJDBCType2ArrowType() {
        assertEquals(Types.MinorType.SMALLINT.getType(), KingbaseUtil.jdbcType2ArrowType("smallint"));
        assertEquals(Types.MinorType.SMALLINT.getType(), KingbaseUtil.jdbcType2ArrowType("int2"));

        assertEquals(Types.MinorType.INT.getType(), KingbaseUtil.jdbcType2ArrowType("int"));
        assertEquals(Types.MinorType.INT.getType(), KingbaseUtil.jdbcType2ArrowType("integer"));
        assertEquals(Types.MinorType.INT.getType(), KingbaseUtil.jdbcType2ArrowType("int4"));
        assertEquals(Types.MinorType.INT.getType(), KingbaseUtil.jdbcType2ArrowType("smallserial"));
        assertEquals(Types.MinorType.INT.getType(), KingbaseUtil.jdbcType2ArrowType("serial2"));
        assertEquals(Types.MinorType.INT.getType(), KingbaseUtil.jdbcType2ArrowType("serial"));
        assertEquals(Types.MinorType.INT.getType(), KingbaseUtil.jdbcType2ArrowType("serial4"));
        assertEquals(Types.MinorType.INT.getType(), KingbaseUtil.jdbcType2ArrowType("bigserial"));
        assertEquals(Types.MinorType.INT.getType(), KingbaseUtil.jdbcType2ArrowType("serial8"));

        assertEquals(Types.MinorType.BIGINT.getType(), KingbaseUtil.jdbcType2ArrowType("bigint"));
        assertEquals(Types.MinorType.BIGINT.getType(), KingbaseUtil.jdbcType2ArrowType("int8"));


        assertEquals(Types.MinorType.FLOAT4.getType(), KingbaseUtil.jdbcType2ArrowType("float4"));
        assertEquals(Types.MinorType.FLOAT4.getType(), KingbaseUtil.jdbcType2ArrowType("real"));


        assertEquals(Types.MinorType.FLOAT8.getType(), KingbaseUtil.jdbcType2ArrowType("double precision"));
        assertEquals(Types.MinorType.FLOAT8.getType(), KingbaseUtil.jdbcType2ArrowType("float8"));
        assertEquals(Types.MinorType.FLOAT8.getType(), KingbaseUtil.jdbcType2ArrowType("numeric"));

        assertEquals(Types.MinorType.VARCHAR.getType(), KingbaseUtil.jdbcType2ArrowType("character varying"));
        assertEquals(Types.MinorType.VARCHAR.getType(), KingbaseUtil.jdbcType2ArrowType("varchar"));
        assertEquals(Types.MinorType.VARCHAR.getType(), KingbaseUtil.jdbcType2ArrowType("text"));
        assertEquals(Types.MinorType.VARCHAR.getType(), KingbaseUtil.jdbcType2ArrowType("char"));
        assertEquals(Types.MinorType.VARCHAR.getType(), KingbaseUtil.jdbcType2ArrowType("enum"));
        assertEquals(Types.MinorType.VARCHAR.getType(), KingbaseUtil.jdbcType2ArrowType("json"));


        assertEquals(Types.MinorType.BIT.getType(), KingbaseUtil.jdbcType2ArrowType("boolean"));
        assertEquals(Types.MinorType.BIT.getType(), KingbaseUtil.jdbcType2ArrowType("bool"));


        assertEquals(Types.MinorType.DATEDAY.getType(), KingbaseUtil.jdbcType2ArrowType("date"));


        assertEquals(Types.MinorType.TIMESTAMPMILLI.getType(), KingbaseUtil.jdbcType2ArrowType("timestamp"));

        assertEquals(Types.MinorType.TIMEMILLI.getType(), KingbaseUtil.jdbcType2ArrowType("time"));


    }


    @Test
    public void testTimestamp() {
        ArrowType type = KingbaseUtil.jdbcType2ArrowType("timestamp");
        assertTrue(type instanceof ArrowType.Timestamp);
        ArrowType.Timestamp ts = (ArrowType.Timestamp) type;
        assertEquals(TimeUnit.MILLISECOND, ts.getUnit());
        // 无时区
        assertNull(ts.getTimezone());
    }

    // 测试 timestamptz（带时区）
    @Test
    public void testTimestamptz() {
        ArrowType type = KingbaseUtil.jdbcType2ArrowType("timestamptz");
        assertTrue(type instanceof ArrowType.Timestamp);
        ArrowType.Timestamp ts = (ArrowType.Timestamp) type;
        assertEquals(TimeUnit.MILLISECOND, ts.getUnit());
        // 必须等于 UTC
        assertEquals("UTC", ts.getTimezone());
    }

    @Test
    public void testBuildMultiRowInsertSql_SimpleInsert() {
        Field column1 = new Field("column1", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field column2 = new Field("column2", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);

        List<Field> columns = Arrays.asList(column1, column2);

        Schema schema = new Schema(columns);

        List<Map<String, Object>> dataList = new ArrayList<>();
        Map<String, Object> row1 = new HashMap<>();
        row1.put("column1", 1_000_000);
        row1.put("column2", "value1");
        dataList.add(row1);

        Map<String, String> partition = Collections.emptyMap();
        DatabaseRecordWriter.SqlWithParams result = KingbaseUtil.buildMultiRowInsertSql(
                "kingbasetable", schema, dataList, partition);

        assertNotNull(result);
        assertNotNull(result.sql);
        assertTrue(result.sql.contains("INSERT INTO kingbasetable"));
        assertTrue(result.sql.contains("column1"));
        assertTrue(result.sql.contains("column2"));
        assertNotNull(result.params);
        assertTrue(result.params.size() == 2);
        assertTrue(result.params.get(1).equals("value1"));
        assertEquals(2, result.params.size());
    }

   /* @Test
    public void testBuildQuerySqlWithPartition() {
        List<String> columns = Arrays.asList("column1", "column2", "column3");
        String partition = "dt=20251210";
        String sql = KingbaseUtil.buildQuerySql("kingbasetable", columns, partition);

        assertTrue(sql.contains("SELECT column1, column2, column3 FROM kingbasetable"));
        assertTrue(sql.contains("WHERE"));
        assertTrue(sql.contains("dt=20251210"));
    }*/


   /* @Test
    void testBuildQuerySql4() {
        String tableName = "table";
        List<String> fields = new ArrayList<>();
        fields.add("column1");
        fields.add("column2");
        fields.add("column3");
        String stmt = KingbaseUtil.buildQuerySql(tableName, fields, "region=us, date=2025-06-26");
        assertEquals("select column1,column2,column3 from table where region='us' and date='2025-06-26'", stmt);
    }*/


}
