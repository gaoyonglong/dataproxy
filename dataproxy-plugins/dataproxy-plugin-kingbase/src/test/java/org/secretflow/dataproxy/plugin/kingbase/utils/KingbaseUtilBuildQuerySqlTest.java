package org.secretflow.dataproxy.plugin.kingbase.utils;

import org.junit.jupiter.api.Test;
import org.secretflow.dataproxy.common.exceptions.DataproxyErrorCode;
import org.secretflow.dataproxy.common.exceptions.DataproxyException;
import org.secretflow.dataproxy.plugin.database.writer.DatabaseRecordWriter;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 *
 * @author chenmingliang
 * @date 2026/01/30
 */
public class KingbaseUtilBuildQuerySqlTest {

    @Test
    public void testBuildQuerySql_WhereClauseIsNull() {
        List<String> columns = Arrays.asList("column1", "column2", "column3");
        DatabaseRecordWriter.SqlWithParams sqlWithParams = KingbaseUtil.buildQuerySql("test_table", columns, null);

        assertEquals("SELECT column1, column2, column3 FROM test_table", sqlWithParams.sql);
        assertTrue(sqlWithParams.params.isEmpty());
    }

    @Test
    public void testBuildQuerySql_WhereClauseIsEmpty() {
        List<String> columns = Arrays.asList("column1", "column2");
        DatabaseRecordWriter.SqlWithParams sqlWithParams = KingbaseUtil.buildQuerySql("test_table", columns, "");

        assertEquals("SELECT column1, column2 FROM test_table", sqlWithParams.sql);
        assertTrue(sqlWithParams.params.isEmpty());
    }

    @Test
    public void testBuildQuerySql_WhereClauseIsBlank() {
        List<String> columns = Arrays.asList("column1", "column2");
        DatabaseRecordWriter.SqlWithParams sqlWithParams = KingbaseUtil.buildQuerySql("test_table", columns, "   ");

        assertEquals("SELECT column1, column2 FROM test_table", sqlWithParams.sql);
        assertTrue(sqlWithParams.params.isEmpty());
    }

    @Test
    public void testBuildQuerySql_SinglePartitionWithComma() {
        List<String> columns = Arrays.asList("column1", "column2");
        DatabaseRecordWriter.SqlWithParams sqlWithParams = KingbaseUtil.buildQuerySql("test_table", columns, "dt=20250101");

        assertEquals("SELECT column1, column2 FROM test_table WHERE dt = ?", sqlWithParams.sql);
        assertEquals(1, sqlWithParams.params.size());
        assertEquals("20250101", sqlWithParams.params.get(0));
    }

    @Test
    public void testBuildQuerySql_MultiplePartitionsWithComma() {
        List<String> columns = Arrays.asList("column1", "column2", "column3");
        DatabaseRecordWriter.SqlWithParams sqlWithParams = KingbaseUtil.buildQuerySql("test_table", columns, "dt=20250101,region=us");

        assertEquals("SELECT column1, column2, column3 FROM test_table WHERE dt = ? and region = ?", sqlWithParams.sql);
        assertEquals(2, sqlWithParams.params.size());
        assertEquals("20250101", sqlWithParams.params.get(0));
        assertEquals("us", sqlWithParams.params.get(1));
    }

    @Test
    public void testBuildQuerySql_ThreePartitions() {
        List<String> columns = Arrays.asList("column1", "column2");
        DatabaseRecordWriter.SqlWithParams sqlWithParams = KingbaseUtil.buildQuerySql("test_table", columns, "dt=20250101,region=us,country=cn");

        assertEquals("SELECT column1, column2 FROM test_table WHERE dt = ? and region = ? and country = ?", sqlWithParams.sql);
        assertEquals(3, sqlWithParams.params.size());
        assertEquals("20250101", sqlWithParams.params.get(0));
        assertEquals("us", sqlWithParams.params.get(1));
        assertEquals("cn", sqlWithParams.params.get(2));
    }

    @Test
    public void testBuildQuerySql_PartitionWithDateFormat() {
        List<String> columns = Arrays.asList("column1");
        DatabaseRecordWriter.SqlWithParams sqlWithParams = KingbaseUtil.buildQuerySql("test_table", columns, "dt=2025-01-30");

        assertEquals("SELECT column1 FROM test_table WHERE dt = ?", sqlWithParams.sql);
        assertEquals(1, sqlWithParams.params.size());
        assertEquals("2025-01-30", sqlWithParams.params.get(0));
    }

    @Test
    public void testBuildQuerySql_PartitionWithUnderscoreAndDot() {
        List<String> columns = Arrays.asList("column1");
        DatabaseRecordWriter.SqlWithParams sqlWithParams = KingbaseUtil.buildQuerySql("test_table", columns, "dt=data_2025.v1");

        assertEquals("SELECT column1 FROM test_table WHERE dt = ?", sqlWithParams.sql);
        assertEquals(1, sqlWithParams.params.size());
        assertEquals("data_2025.v1", sqlWithParams.params.get(0));
    }

    @Test
    public void testBuildQuerySql_PartitionWithHyphen() {
        List<String> columns = Arrays.asList("column1");
        DatabaseRecordWriter.SqlWithParams sqlWithParams = KingbaseUtil.buildQuerySql("test_table", columns, "region=us-west");

        assertEquals("SELECT column1 FROM test_table WHERE region = ?", sqlWithParams.sql);
        assertEquals(1, sqlWithParams.params.size());
        assertEquals("us-west", sqlWithParams.params.get(0));
    }

    @Test
    public void testBuildQuerySql_SingleField() {
        List<String> columns = Collections.singletonList("column1");
        DatabaseRecordWriter.SqlWithParams sqlWithParams = KingbaseUtil.buildQuerySql("test_table", columns, "dt=20250101");

        assertEquals("SELECT column1 FROM test_table WHERE dt = ?", sqlWithParams.sql);
        assertEquals(1, sqlWithParams.params.size());
        assertEquals("20250101", sqlWithParams.params.get(0));
    }

    @Test
    public void testBuildQuerySql_InvalidTableNameWithSpecialChars() {
        List<String> columns = Arrays.asList("column1", "column2");
        DataproxyException exception = assertThrows(DataproxyException.class, () -> {
            KingbaseUtil.buildQuerySql("test-table", columns, "dt=20250101");
        });
        assertEquals(DataproxyErrorCode.PARAMS_UNRELIABLE, exception.getErrorCode());
        assertTrue(exception.getMessage().contains("Invalid tableName:test-table"));
    }

    @Test
    public void testBuildQuerySql_InvalidTableNameWithSpace() {
        List<String> columns = Arrays.asList("column1", "column2");
        DataproxyException exception = assertThrows(DataproxyException.class, () -> {
            KingbaseUtil.buildQuerySql("test table", columns, "dt=20250101");
        });
        assertEquals(DataproxyErrorCode.PARAMS_UNRELIABLE, exception.getErrorCode());
        assertTrue(exception.getMessage().contains("Invalid tableName:test table"));
    }

    @Test
    public void testBuildQuerySql_InvalidTableNameStartsWithDot() {
        List<String> columns = Arrays.asList("column1", "column2");
        DataproxyException exception = assertThrows(DataproxyException.class, () -> {
            KingbaseUtil.buildQuerySql(".test_table", columns, "dt=20250101");
        });
        assertEquals(DataproxyErrorCode.PARAMS_UNRELIABLE, exception.getErrorCode());
        assertTrue(exception.getMessage().contains("Invalid tableName:.test_table"));
    }

    @Test
    public void testBuildQuerySql_InvalidTableNameEndsWithDot() {
        List<String> columns = Arrays.asList("column1", "column2");
        DataproxyException exception = assertThrows(DataproxyException.class, () -> {
            KingbaseUtil.buildQuerySql("test_table.", columns, "dt=20250101");
        });
        assertEquals(DataproxyErrorCode.PARAMS_UNRELIABLE, exception.getErrorCode());
        assertTrue(exception.getMessage().contains("Invalid tableName:test_table."));
    }

    @Test
    public void testBuildQuerySql_InvalidTableNameWithDoubleDots() {
        List<String> columns = Arrays.asList("column1", "column2");
        DataproxyException exception = assertThrows(DataproxyException.class, () -> {
            KingbaseUtil.buildQuerySql("test..table", columns, "dt=20250101");
        });
        assertEquals(DataproxyErrorCode.PARAMS_UNRELIABLE, exception.getErrorCode());
        assertTrue(exception.getMessage().contains("Invalid tableName:test..table"));
    }

    @Test
    public void testBuildQuerySql_InvalidFieldNameWithSpecialChars() {
        List<String> columns = Arrays.asList("column1", "column-2");
        DataproxyException exception = assertThrows(DataproxyException.class, () -> {
            KingbaseUtil.buildQuerySql("test_table", columns, "dt=20250101");
        });
        assertEquals(DataproxyErrorCode.PARAMS_UNRELIABLE, exception.getErrorCode());
        assertTrue(exception.getMessage().contains("Invalid field name:column-2"));
    }

    /*@Test
    public void testBuildQuerySql_InvalidPartitionKey() {
        List<String> columns = Arrays.asList("column1");
        DataproxyException exception = assertThrows(DataproxyException.class, () -> {
            KingbaseUtil.buildQuerySql("test_table", columns, "date-time=20250101,region=us");
        });
        assertEquals(DataproxyErrorCode.PARAMS_UNRELIABLE, exception.getErrorCode());
        assertTrue(exception.getMessage().contains("Invalid partition key:date-time"));
    }*/

  /*  @Test
    public void testBuildQuerySql_InvalidPartitionValueWithSemicolon() {
        List<String> columns = Arrays.asList("column1");
        DataproxyException exception = assertThrows(DataproxyException.class, () -> {
            KingbaseUtil.buildQuerySql("test_table", columns, "dt=2025;01;01");
        });
        assertEquals(DataproxyErrorCode.PARAMS_UNRELIABLE, exception.getErrorCode());
        assertTrue(exception.getMessage().contains("Invalid partition value:2025;01;01"));
    }*/

    /*@Test
    public void testBuildQuerySql_InvalidPartitionValueWithQuote() {
        List<String> columns = Arrays.asList("column1");
        DataproxyException exception = assertThrows(DataproxyException.class, () -> {
            KingbaseUtil.buildQuerySql("test_table", columns, "dt=202501'01");
        });
        assertEquals(DataproxyErrorCode.PARAMS_UNRELIABLE, exception.getErrorCode());
        assertTrue(exception.getMessage().contains("Invalid partition value:202501'01"));
    }*/

   /* @Test
    public void testBuildQuerySql_PartitionValueWithEscapedQuote() {
        List<String> columns = Arrays.asList("column1");
        DataproxyException exception = assertThrows(DataproxyException.class, () -> {
            KingbaseUtil.buildQuerySql("test_table", columns, "name=O'Brien");
        });
        assertEquals(DataproxyErrorCode.PARAMS_UNRELIABLE, exception.getErrorCode());
    }*/

    @Test
    public void testBuildQuerySql_EmptyFieldsList() {
        List<String> columns = Collections.emptyList();
        DatabaseRecordWriter.SqlWithParams sqlWithParams = KingbaseUtil.buildQuerySql("test_table", columns, "dt=20250101");

        assertEquals("SELECT  FROM test_table WHERE dt = ?", sqlWithParams.sql);
        assertEquals(1, sqlWithParams.params.size());
        assertEquals("20250101", sqlWithParams.params.get(0));
    }

    @Test
    public void testBuildQuerySql_UnderscoreAndNumericIdentifiers() {
        List<String> columns = Arrays.asList("column_1", "column_2", "field3");
        DatabaseRecordWriter.SqlWithParams sqlWithParams = KingbaseUtil.buildQuerySql("test_table_123", columns, "dt=20250101,region=us");

        assertEquals("SELECT column_1, column_2, field3 FROM test_table_123 WHERE dt = ? and region = ?", sqlWithParams.sql);
        assertEquals(2, sqlWithParams.params.size());
        assertEquals("20250101", sqlWithParams.params.get(0));
        assertEquals("us", sqlWithParams.params.get(1));
    }

    @Test
    public void testBuildQuerySql_ValidAlphanumericPartition() {
        List<String> columns = Arrays.asList("column1");
        DatabaseRecordWriter.SqlWithParams sqlWithParams = KingbaseUtil.buildQuerySql("test_table", columns, "data_type=test_v1.2");

        assertEquals("SELECT column1 FROM test_table WHERE data_type = ?", sqlWithParams.sql);
        assertEquals(1, sqlWithParams.params.size());
        assertEquals("test_v1.2", sqlWithParams.params.get(0));
    }

    @Test
    public void testBuildQuerySql_LongPartitionValue() {
        List<String> columns = Arrays.asList("column1");
        String longValue = "very_long_partition_name_with_multiple_characters_v2025_01_30_us_east";
        DatabaseRecordWriter.SqlWithParams sqlWithParams = KingbaseUtil.buildQuerySql("test_table", columns, "partition_key=" + longValue);

        assertEquals("SELECT column1 FROM test_table WHERE partition_key = ?", sqlWithParams.sql);
        assertEquals(1, sqlWithParams.params.size());
        assertEquals(longValue, sqlWithParams.params.get(0));
    }


    @Test
    public void testBuildQuerySql_WithMultipleValidPartitions() {
        org.mockito.MockedStatic<org.secretflow.dataproxy.plugin.database.writer.DatabaseRecordWriter> mockedStatic =
                org.mockito.Mockito.mockStatic(org.secretflow.dataproxy.plugin.database.writer.DatabaseRecordWriter.class);

        try {
            java.util.Map<String, String> mockPartitionSpec = new java.util.LinkedHashMap<>();
            mockPartitionSpec.put("dt", "20250101");
            mockPartitionSpec.put("region", "us");

            mockedStatic.when(() ->
                org.secretflow.dataproxy.plugin.database.writer.DatabaseRecordWriter.parsePartition(
                    org.mockito.ArgumentMatchers.anyString()
                )
            ).thenAnswer(invocation -> mockPartitionSpec);

            List<String> columns = Arrays.asList("column1", "column2");
            DatabaseRecordWriter.SqlWithParams sqlWithParams = KingbaseUtil.buildQuerySql("test_table", columns, "dt=20250101,region=us");

            assertEquals("SELECT column1, column2 FROM test_table WHERE dt = ? and region = ?", sqlWithParams.sql);
            assertEquals(2, sqlWithParams.params.size());
            assertEquals("20250101", sqlWithParams.params.get(0));
            assertEquals("us", sqlWithParams.params.get(1));
        } finally {
            mockedStatic.close();
        }
    }

    @Test
    public void testBuildQuerySql_WithValidComplexPartitionValue() {
        org.mockito.MockedStatic<org.secretflow.dataproxy.plugin.database.writer.DatabaseRecordWriter> mockedStatic =
                org.mockito.Mockito.mockStatic(org.secretflow.dataproxy.plugin.database.writer.DatabaseRecordWriter.class);

        try {
            java.util.Map<String, String> mockPartitionSpec = new java.util.LinkedHashMap<>();
            mockPartitionSpec.put("dt", "2025_01_30");
            mockPartitionSpec.put("region", "us-west");
            mockPartitionSpec.put("version", "v1.2.3");

            mockedStatic.when(() ->
                org.secretflow.dataproxy.plugin.database.writer.DatabaseRecordWriter.parsePartition(
                    org.mockito.ArgumentMatchers.anyString()
                )
            ).thenAnswer(invocation -> mockPartitionSpec);

            List<String> columns = Arrays.asList("column1");
            DatabaseRecordWriter.SqlWithParams sqlWithParams = KingbaseUtil.buildQuerySql("test_table", columns, "dt=2025_01_30,region=us-west,version=v1.2.3");

            assertEquals("SELECT column1 FROM test_table WHERE dt = ? and region = ? and version = ?", sqlWithParams.sql);
            assertEquals(3, sqlWithParams.params.size());
            assertEquals("2025_01_30", sqlWithParams.params.get(0));
            assertEquals("us-west", sqlWithParams.params.get(1));
            assertEquals("v1.2.3", sqlWithParams.params.get(2));
        } finally {
            mockedStatic.close();
        }
    }

    @Test
    public void testBuildQuerySql_WithPartitionKeyStartsWithNumber() {
        org.mockito.MockedStatic<org.secretflow.dataproxy.plugin.database.writer.DatabaseRecordWriter> mockedStatic =
                org.mockito.Mockito.mockStatic(org.secretflow.dataproxy.plugin.database.writer.DatabaseRecordWriter.class);

        try {
            java.util.Map<String, String> mockPartitionSpec = new java.util.LinkedHashMap<>();
            mockPartitionSpec.put("1date", "20250101");
            mockPartitionSpec.put("region", "us");

            mockedStatic.when(() ->
                org.secretflow.dataproxy.plugin.database.writer.DatabaseRecordWriter.parsePartition(
                    org.mockito.ArgumentMatchers.anyString()
                )
            ).thenAnswer(invocation -> mockPartitionSpec);

            List<String> columns = Arrays.asList("column1");
            DatabaseRecordWriter.SqlWithParams sqlWithParams = KingbaseUtil.buildQuerySql("test_table", columns, "1date=20250101,region=us");

            assertEquals("SELECT column1 FROM test_table WHERE 1date = ? and region = ?", sqlWithParams.sql);
            assertEquals(2, sqlWithParams.params.size());
            assertEquals("20250101", sqlWithParams.params.get(0));
            assertEquals("us", sqlWithParams.params.get(1));
        } finally {
            mockedStatic.close();
        }
    }

    @Test
    public void testBuildQuerySql_WithTwoPartitionsFullValidation() {
        org.mockito.MockedStatic<org.secretflow.dataproxy.plugin.database.writer.DatabaseRecordWriter> mockedStatic =
                org.mockito.Mockito.mockStatic(org.secretflow.dataproxy.plugin.database.writer.DatabaseRecordWriter.class);

        try {
            java.util.Map<String, String> mockPartitionSpec = new java.util.LinkedHashMap<>();
            mockPartitionSpec.put("dt", "20250101");
            mockPartitionSpec.put("region", "us");

            mockedStatic.when(() ->
                org.secretflow.dataproxy.plugin.database.writer.DatabaseRecordWriter.parsePartition(
                    org.mockito.ArgumentMatchers.anyString()
                )
            ).thenAnswer(invocation -> mockPartitionSpec);

            List<String> columns = Arrays.asList("column1", "column2");
            DatabaseRecordWriter.SqlWithParams sqlWithParams = KingbaseUtil.buildQuerySql("test_table", columns, "dt=20250101,region=us");

            assertEquals("SELECT column1, column2 FROM test_table WHERE dt = ? and region = ?", sqlWithParams.sql);
            assertEquals(2, sqlWithParams.params.size());
            assertEquals("20250101", sqlWithParams.params.get(0));
            assertEquals("us", sqlWithParams.params.get(1));
        } finally {
            mockedStatic.close();
        }
    }
}