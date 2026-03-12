/*
 * Copyright 2026 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.secretflow.dataproxy.plugin.kingbase.utils;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.secretflow.dataproxy.common.exceptions.DataproxyErrorCode;
import org.secretflow.dataproxy.common.exceptions.DataproxyException;
import org.secretflow.dataproxy.plugin.database.writer.DatabaseRecordWriter;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 *
 * @author chenmingliang
 * @date 2026/01/30
 */
public class KingbaseUtilBuildMultiRowInsertSqlTest {

    @Test
    public void testBuildMultiRowInsertSql_WithNullDataList() {
        String tableName = "test_table";
        Field field1 = new Field("column1", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Schema schema = new Schema(Arrays.asList(field1));

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            KingbaseUtil.buildMultiRowInsertSql(tableName, schema, null, null);
        });

        assertEquals("No data to insert", exception.getMessage());
    }

    @Test
    public void testBuildMultiRowInsertSql_WithEmptyDataList() {
        String tableName = "test_table";
        Field field1 = new Field("column1", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Schema schema = new Schema(Arrays.asList(field1));

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            KingbaseUtil.buildMultiRowInsertSql(tableName, schema, Collections.emptyList(), null);
        });

        assertEquals("No data to insert", exception.getMessage());
    }

    @Test
    public void testBuildMultiRowInsertSql_SingleRowWithoutPartition() {
        String tableName = "test_table";
        Field field1 = new Field("column1", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field field2 = new Field("column2", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Schema schema = new Schema(Arrays.asList(field1, field2));

        List<Map<String, Object>> dataList = new ArrayList<>();
        Map<String, Object> row1 = new HashMap<>();
        row1.put("column1", 100);
        row1.put("column2", "value1");
        dataList.add(row1);

        DatabaseRecordWriter.SqlWithParams result = KingbaseUtil.buildMultiRowInsertSql(
                tableName, schema, dataList, null);

        assertNotNull(result);
        assertTrue(result.sql.contains("INSERT INTO test_table"));
        assertTrue(result.sql.contains("(column1, column2)"));
        assertTrue(result.sql.contains("VALUES (?, ?)"));
        assertEquals(2, result.params.size());
        assertEquals(100, result.params.get(0));
        assertEquals("value1", result.params.get(1));
    }

    @Test
    public void testBuildMultiRowInsertSql_MultipleRowsWithoutPartition() {
        String tableName = "test_table";
        Field field1 = new Field("column1", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field field2 = new Field("column2", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Schema schema = new Schema(Arrays.asList(field1, field2));

        List<Map<String, Object>> dataList = new ArrayList<>();
        Map<String, Object> row1 = new HashMap<>();
        row1.put("column1", 100);
        row1.put("column2", "value1");
        dataList.add(row1);

        Map<String, Object> row2 = new HashMap<>();
        row2.put("column1", 200);
        row2.put("column2", "value2");
        dataList.add(row2);

        Map<String, Object> row3 = new HashMap<>();
        row3.put("column1", 300);
        row3.put("column2", "value3");
        dataList.add(row3);

        DatabaseRecordWriter.SqlWithParams result = KingbaseUtil.buildMultiRowInsertSql(
                tableName, schema, dataList, null);

        assertNotNull(result);
        assertTrue(result.sql.contains("INSERT INTO test_table"));
        assertTrue(result.sql.contains("(column1, column2) VALUES"));
        assertTrue(result.sql.contains("(?, ?), (?, ?), (?, ?)"));
        assertEquals(6, result.params.size());
        assertEquals(100, result.params.get(0));
        assertEquals("value1", result.params.get(1));
        assertEquals(200, result.params.get(2));
        assertEquals("value2", result.params.get(3));
        assertEquals(300, result.params.get(4));
        assertEquals("value3", result.params.get(5));
    }

    @Test
    public void testBuildMultiRowInsertSql_WithPartition_KeysInData() {
        String tableName = "test_table";
        Field field1 = new Field("column1", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field field2 = new Field("column2", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Field field3 = new Field("partition_key", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Schema schema = new Schema(Arrays.asList(field1, field2, field3));

        List<Map<String, Object>> dataList = new ArrayList<>();
        Map<String, Object> row1 = new HashMap<>();
        row1.put("column1", 100);
        row1.put("column2", "value1");
        row1.put("partition_key", "part1");
        dataList.add(row1);

        Map<String, String> partition = new HashMap<>();
        partition.put("partition_key", "part1");

        DatabaseRecordWriter.SqlWithParams result = KingbaseUtil.buildMultiRowInsertSql(
                tableName, schema, dataList, partition);

        assertNotNull(result);
        assertTrue(result.sql.contains("INSERT INTO test_table"));
        assertFalse(result.sql.contains("partition_key"));
        assertTrue(result.sql.contains("(column1, column2)"));
        assertEquals(3, result.params.size());
        assertEquals(100, result.params.get(0));
        assertEquals("value1", result.params.get(1));
        assertEquals("part1", result.params.get(2));
    }

    @Test
    public void testBuildMultiRowInsertSql_WithPartition_SingleRow() {
        String tableName = "test_table";
        Field field1 = new Field("column1", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field field2 = new Field("column2", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Field field3 = new Field("dt", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Schema schema = new Schema(Arrays.asList(field1, field2, field3));

        List<Map<String, Object>> dataList = new ArrayList<>();
        Map<String, Object> row1 = new HashMap<>();
        row1.put("column1", 100);
        row1.put("column2", "value1");
        row1.put("dt", "20250101");
        dataList.add(row1);

        Map<String, String> partition = new HashMap<>();
        partition.put("dt", "20250101");

        DatabaseRecordWriter.SqlWithParams result = KingbaseUtil.buildMultiRowInsertSql(
                tableName, schema, dataList, null);

        assertNotNull(result);
        assertTrue(result.sql.contains("(column1, column2, dt)"));
        assertEquals(3, result.sql.split("\\?").length - 1);
        assertEquals(3, result.params.size());
    }

    @Test
    public void testBuildMultiRowInsertSql_MultipleRows_SinglePartition() {
        String tableName = "test_table";
        Field field1 = new Field("column1", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field field2 = new Field("dt", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Schema schema = new Schema(Arrays.asList(field1, field2));

        List<Map<String, Object>> dataList = new ArrayList<>();
        Map<String, Object> row1 = new HashMap<>();
        row1.put("column1", 100);
        row1.put("dt", "20250101");
        dataList.add(row1);

        Map<String, Object> row2 = new HashMap<>();
        row2.put("column1", 200);
        row2.put("dt", "20250102");
        dataList.add(row2);

        Map<String, String> partition = new HashMap<>();
        partition.put("dt", "20250101");

        DatabaseRecordWriter.SqlWithParams result = KingbaseUtil.buildMultiRowInsertSql(
                tableName, schema, dataList, partition);

        assertNotNull(result);
        assertFalse(result.sql.contains("dt"));
        assertTrue(result.sql.contains("(column1)"));
        assertEquals(2, result.sql.split("\\?").length - 1);
        assertEquals(4, result.params.size());
        assertEquals(100, result.params.get(0));
        assertEquals(200, result.params.get(1));
        assertEquals("20250101", result.params.get(2));
        assertEquals("20250102", result.params.get(3));
    }

    @Test
    public void testBuildMultiRowInsertSql_InvalidTableName() {
        String tableName = "test-table";
        Field field1 = new Field("column1", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Schema schema = new Schema(Arrays.asList(field1));

        List<Map<String, Object>> dataList = new ArrayList<>();
        Map<String, Object> row1 = new HashMap<>();
        row1.put("column1", 100);
        dataList.add(row1);

        DataproxyException exception = assertThrows(DataproxyException.class, () -> {
            KingbaseUtil.buildMultiRowInsertSql(tableName, schema, dataList, null);
        });

        assertEquals(DataproxyErrorCode.PARAMS_UNRELIABLE, exception.getErrorCode());
        assertTrue(exception.getMessage().contains("Invalid tableName:test-table"));
    }

    @Test
    public void testBuildMultiRowInsertSql_InvalidFieldName() {
        String tableName = "test_table";
        Field field1 = new Field("column-1", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Schema schema = new Schema(Arrays.asList(field1));

        List<Map<String, Object>> dataList = new ArrayList<>();
        Map<String, Object> row1 = new HashMap<>();
        row1.put("column-1", 100);
        dataList.add(row1);

        DataproxyException exception = assertThrows(DataproxyException.class, () -> {
            KingbaseUtil.buildMultiRowInsertSql(tableName, schema, dataList, null);
        });

        assertEquals(DataproxyErrorCode.PARAMS_UNRELIABLE, exception.getErrorCode());
        assertTrue(exception.getMessage().contains("Invalid field name:column-1"));
    }

    @Test
    public void testBuildMultiRowInsertSql_InvalidPartitionKey() {
        String tableName = "test_table";
        Field field1 = new Field("column1", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Schema schema = new Schema(Arrays.asList(field1));

        List<Map<String, Object>> dataList = new ArrayList<>();
        Map<String, Object> row1 = new HashMap<>();
        row1.put("column1", 100);
        dataList.add(row1);

        Map<String, String> partition = new HashMap<>();
        partition.put("partition-key", "part1");

        DataproxyException exception = assertThrows(DataproxyException.class, () -> {
            KingbaseUtil.buildMultiRowInsertSql(tableName, schema, dataList, partition);
        });

        assertEquals(DataproxyErrorCode.PARAMS_UNRELIABLE, exception.getErrorCode());
        assertTrue(exception.getMessage().contains("Invalid partition key:partition-key"));
    }

    @Test
    public void testBuildMultiRowInsertSql_EmptyPartition() {
        String tableName = "test_table";
        Field field1 = new Field("column1", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field field2 = new Field("column2", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Schema schema = new Schema(Arrays.asList(field1, field2));

        List<Map<String, Object>> dataList = new ArrayList<>();
        Map<String, Object> row1 = new HashMap<>();
        row1.put("column1", 100);
        row1.put("column2", "value1");
        dataList.add(row1);

        DatabaseRecordWriter.SqlWithParams result = KingbaseUtil.buildMultiRowInsertSql(
                tableName, schema, dataList, Collections.emptyMap());

        assertNotNull(result);
        assertTrue(result.sql.contains("(column1, column2)"));
        assertEquals(2, result.params.size());
    }

    @Test
    public void testBuildMultiRowInsertSql_VariousDataTypes() {
        String tableName = "test_table";
        Field field1 = new Field("int_col", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field field2 = new Field("bigint_col", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field field3 = new Field("float_col", FieldType.nullable(new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE)), null);
        Field field4 = new Field("double_col", FieldType.nullable(new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)), null);
        Field field5 = new Field("string_col", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Field field6 = new Field("bool_col", FieldType.nullable(ArrowType.Bool.INSTANCE), null);
        Schema schema = new Schema(Arrays.asList(field1, field2, field3, field4, field5, field6));

        List<Map<String, Object>> dataList = new ArrayList<>();
        Map<String, Object> row1 = new HashMap<>();
        row1.put("int_col", 100);
        row1.put("bigint_col", 1234567890L);
        row1.put("float_col", 3.14f);
        row1.put("double_col", 2.71828);
        row1.put("string_col", "test_value");
        row1.put("bool_col", true);
        dataList.add(row1);

        DatabaseRecordWriter.SqlWithParams result = KingbaseUtil.buildMultiRowInsertSql(
                tableName, schema, dataList, null);

        assertNotNull(result);
        assertTrue(result.sql.contains("(int_col, bigint_col, float_col, double_col, string_col, bool_col)"));
        assertTrue(result.sql.contains("VALUES (?, ?, ?, ?, ?, ?)"));
        assertEquals(6, result.params.size());
        assertEquals(100, result.params.get(0));
        assertEquals(1234567890L, result.params.get(1));
        assertEquals(3.14f, result.params.get(2));
        assertEquals(2.71828, result.params.get(3));
        assertEquals("test_value", result.params.get(4));
        assertEquals(true, result.params.get(5));
    }

    @Test
    public void testBuildMultiRowInsertSql_WithNullValues() {
        String tableName = "test_table";
        Field field1 = new Field("column1", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field field2 = new Field("column2", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Schema schema = new Schema(Arrays.asList(field1, field2));

        List<Map<String, Object>> dataList = new ArrayList<>();
        Map<String, Object> row1 = new HashMap<>();
        row1.put("column1", 100);
        row1.put("column2", null);
        dataList.add(row1);

        Map<String, Object> row2 = new HashMap<>();
        row2.put("column1", null);
        row2.put("column2", "value2");
        dataList.add(row2);

        DatabaseRecordWriter.SqlWithParams result = KingbaseUtil.buildMultiRowInsertSql(
                tableName, schema, dataList, null);

        assertNotNull(result);
        assertEquals(4, result.params.size());
        assertEquals(100, result.params.get(0));
        assertNull(result.params.get(1));
        assertNull(result.params.get(2));
        assertEquals("value2", result.params.get(3));
    }

    @Test
    public void testBuildMultiRowInsertSql_SingleColumn() {
        String tableName = "test_table";
        Field field1 = new Field("column1", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Schema schema = new Schema(Arrays.asList(field1));

        List<Map<String, Object>> dataList = new ArrayList<>();
        Map<String, Object> row1 = new HashMap<>();
        row1.put("column1", 100);
        dataList.add(row1);

        DatabaseRecordWriter.SqlWithParams result = KingbaseUtil.buildMultiRowInsertSql(
                tableName, schema, dataList, null);

        assertNotNull(result);
        assertTrue(result.sql.contains("(column1)"));
        assertTrue(result.sql.contains("VALUES (?)"));
        assertEquals(1, result.params.size());
        assertEquals(100, result.params.get(0));
    }

    @Test
    public void testBuildMultiRowInsertSql_ManyRows() {
        String tableName = "test_table";
        Field field1 = new Field("column1", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field field2 = new Field("column2", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Schema schema = new Schema(Arrays.asList(field1, field2));

        List<Map<String, Object>> dataList = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("column1", i);
            row.put("column2", "value" + i);
            dataList.add(row);
        }

        DatabaseRecordWriter.SqlWithParams result = KingbaseUtil.buildMultiRowInsertSql(
                tableName, schema, dataList, null);

        assertNotNull(result);
        // 应该有 100 个 (?, ?) 组合
        long paramCount = result.sql.chars().filter(ch -> ch == '?').count();
        assertEquals(200, paramCount);
        assertEquals(200, result.params.size());
    }

    @Test
    public void testBuildMultiRowInsertSql_MultiplePartitions() {
        String tableName = "test_table";
        Field field1 = new Field("column1", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field field2 = new Field("dt", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Field field3 = new Field("region", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Schema schema = new Schema(Arrays.asList(field1, field2, field3));

        List<Map<String, Object>> dataList = new ArrayList<>();
        Map<String, Object> row1 = new HashMap<>();
        row1.put("column1", 100);
        row1.put("dt", "20250101");
        row1.put("region", "us");
        dataList.add(row1);

        Map<String, String> partition = new HashMap<>();
        partition.put("dt", "20250101");
        partition.put("region", "us");

        DatabaseRecordWriter.SqlWithParams result = KingbaseUtil.buildMultiRowInsertSql(
                tableName, schema, dataList, partition);

        assertNotNull(result);
        // Both partition fields should be filtered out
        assertFalse(result.sql.contains("dt"));
        assertFalse(result.sql.contains("region"));
        assertTrue(result.sql.contains("(column1)"));
        assertEquals(1, result.sql.split("\\?").length - 1);
        assertEquals(3, result.params.size());
        assertEquals(100, result.params.get(0));
        assertTrue(result.params.get(1) != null);
        assertTrue(result.params.get(2) != null);
    }

    @Test
    public void testBuildMultiRowInsertSql_TableNameWithUnderscoreAndNumber() {
        String tableName = "test_table_123";
        Field field1 = new Field("column1", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Schema schema = new Schema(Arrays.asList(field1));

        List<Map<String, Object>> dataList = new ArrayList<>();
        Map<String, Object> row1 = new HashMap<>();
        row1.put("column1", 100);
        dataList.add(row1);

        DatabaseRecordWriter.SqlWithParams result = KingbaseUtil.buildMultiRowInsertSql(
                tableName, schema, dataList, null);

        assertNotNull(result);
        assertTrue(result.sql.contains("INSERT INTO test_table_123"));
    }

    @Test
    public void testBuildMultiRowInsertSql_FieldNameWithUnderscoreAndNumber() {
        String tableName = "test_table";
        Field field1 = new Field("column_1", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field field2 = new Field("column_2", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Schema schema = new Schema(Arrays.asList(field1, field2));

        List<Map<String, Object>> dataList = new ArrayList<>();
        Map<String, Object> row1 = new HashMap<>();
        row1.put("column_1", 100);
        row1.put("column_2", "value1");
        dataList.add(row1);

        DatabaseRecordWriter.SqlWithParams result = KingbaseUtil.buildMultiRowInsertSql(
                tableName, schema, dataList, null);

        assertNotNull(result);
        assertTrue(result.sql.contains("(column_1, column_2)"));
        assertEquals(2, result.params.size());
    }

    @Test
    public void testBuildMultiRowInsertSql_MissingFieldValue() {
        String tableName = "test_table";
        Field field1 = new Field("column1", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field field2 = new Field("column2", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Schema schema = new Schema(Arrays.asList(field1, field2));

        List<Map<String, Object>> dataList = new ArrayList<>();
        Map<String, Object> row1 = new HashMap<>();
        row1.put("column1", 100);
        dataList.add(row1);

        DatabaseRecordWriter.SqlWithParams result = KingbaseUtil.buildMultiRowInsertSql(
                tableName, schema, dataList, null);

        assertNotNull(result);
        assertEquals(2, result.params.size());
        assertEquals(100, result.params.get(0));
        assertNull(result.params.get(1));
    }

    @Test
    public void testBuildMultiRowInsertSql_WithPartitionKeyNotInSchema() {
        String tableName = "test_table";
        Field field1 = new Field("column1", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Schema schema = new Schema(Arrays.asList(field1));

        List<Map<String, Object>> dataList = new ArrayList<>();
        Map<String, Object> row1 = new HashMap<>();
        row1.put("column1", 100);
        dataList.add(row1);

        Map<String, String> partition = new HashMap<>();
        partition.put("dt", "20250101");

        DatabaseRecordWriter.SqlWithParams result = KingbaseUtil.buildMultiRowInsertSql(
                tableName, schema, dataList, partition);

        assertNotNull(result);
        assertTrue(result.sql.contains("(column1)"));
        assertEquals(1, result.params.size());
        assertEquals(100, result.params.get(0));
    }

    @Test
    public void testBuildMultiRowInsertSql_SqlFormat() {
        String tableName = "test_table";
        Field field1 = new Field("column1", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field field2 = new Field("column2", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Schema schema = new Schema(Arrays.asList(field1, field2));

        List<Map<String, Object>> dataList = new ArrayList<>();
        Map<String, Object> row1 = new HashMap<>();
        row1.put("column1", 100);
        row1.put("column2", "value1");
        dataList.add(row1);

        DatabaseRecordWriter.SqlWithParams result = KingbaseUtil.buildMultiRowInsertSql(
                tableName, schema, dataList, null);

        String expectedSql = "INSERT INTO test_table (column1, column2) VALUES (?, ?)";
        assertEquals(expectedSql, result.sql);
    }

    @Test
    public void testBuildMultiRowInsertSql_ExtraFieldsInData() {
        String tableName = "test_table";
        Field field1 = new Field("column1", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Schema schema = new Schema(Arrays.asList(field1));

        List<Map<String, Object>> dataList = new ArrayList<>();
        Map<String, Object> row1 = new HashMap<>();
        row1.put("column1", 100);
        row1.put("extra_column", "ignored"); // 不在 schema 中的字段
        dataList.add(row1);

        DatabaseRecordWriter.SqlWithParams result = KingbaseUtil.buildMultiRowInsertSql(
                tableName, schema, dataList, null);

        assertNotNull(result);
        assertFalse(result.sql.contains("extra_column"));
        assertEquals(1, result.params.size());
        assertEquals(100, result.params.get(0));
    }
}