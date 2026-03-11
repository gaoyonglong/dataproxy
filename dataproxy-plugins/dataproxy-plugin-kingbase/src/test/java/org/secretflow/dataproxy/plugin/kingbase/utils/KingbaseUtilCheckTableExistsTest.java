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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.secretflow.dataproxy.common.exceptions.DataproxyErrorCode;
import org.secretflow.dataproxy.common.exceptions.DataproxyException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 *
 * @author chenmingliang
 * @date 2026/01/30
 */
public class KingbaseUtilCheckTableExistsTest {

    @Mock
    private Connection connection;

    @Mock
    private Statement statement;

    @Mock
    private ResultSet resultSet;

    @BeforeEach
    public void setUp() throws SQLException {
        MockitoAnnotations.openMocks(this);
    }

    // Test: table exists (count > 0)
    @Test
    public void testCheckTableExists_TableExists() throws SQLException {
        String tableName = "test_table";
        
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getInt(1)).thenReturn(1);

        boolean result = KingbaseUtil.checkTableExists(connection, tableName);
        
        assertTrue(result);
        verify(statement).executeQuery(contains("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'test_table'"));
        verify(resultSet).close();
        verify(statement).close();
    }

    //  Test: table does not exist (count = 0)
    @Test
    public void testCheckTableExists_TableDoesNotExist() throws SQLException {
        String tableName = "non_existent_table";
        
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getInt(1)).thenReturn(0);

        boolean result = KingbaseUtil.checkTableExists(connection, tableName);
        
        assertFalse(result);
        verify(resultSet).close();
        verify(statement).close();
    }

    // Test: result set has no rows
    @Test
    public void testCheckTable_NoRows() throws SQLException {
        String tableName = "test_table";
        
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        boolean result = KingbaseUtil.checkTableExists(connection, tableName);
        
        assertFalse(result);
        verify(resultSet).close();
        verify(statement).close();
    }

    //  Test: SQL exception during query execution
    @Test
    public void testCheckTableExists_SqlException() throws SQLException {
        String tableName = "test_table";
        SQLException sqlException = new SQLException("Connection failed");

        when(connection.createStatement()).thenThrow(sqlException);

        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            KingbaseUtil.checkTableExists(connection, tableName);
        });
        
        assertTrue(exception.getCause() instanceof SQLException);
        assertEquals("Connection failed", exception.getCause().getMessage());
    }

    //  Test: SQLException during ResultSet close
    @Test
    public void testCheckTableExists_ResultSetCloseException() throws SQLException {
        String tableName = "test_table";
        SQLException closeException = new SQLException("Close failed");

        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getInt(1)).thenReturn(1);
        doThrow(closeException).when(resultSet).close();

        // Should throw RuntimeException due to close failure
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            KingbaseUtil.checkTableExists(connection, tableName);
        });
        
        assertTrue(exception.getCause() instanceof SQLException);
        assertEquals("Close failed", exception.getCause().getMessage());
    }

    // Test: SQLException during Statement close
    @Test
    public void testCheckTableExists_StatementCloseException() throws SQLException {
        String tableName = "test_table";
        SQLException closeException = new SQLException("Statement close failed");

        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getInt(1)).thenReturn(1);
        doThrow(closeException).when(statement).close();

        // Should throw RuntimeException due to close failure
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            KingbaseUtil.checkTableExists(connection, tableName);
        });
        
        assertTrue(exception.getCause() instanceof SQLException);
        assertEquals("Statement close failed", exception.getCause().getMessage());
    }

    //  Test: valid table name with only letters
    @Test
    public void testCheckTableExists_ValidTableName_LettersOnly() throws SQLException {
        String tableName = "validtable";
        
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getInt(1)).thenReturn(1);

        boolean result = KingbaseUtil.checkTableExists(connection, tableName);
        
        assertTrue(result);
    }

    // [] Test: valid table name with letters and numbers
    @Test
    public void testCheckTableExists_ValidTableName_LettersAndNumbers() throws SQLException {
        String tableName = "table123";
        
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getInt(1)).thenReturn(1);

        boolean result = KingbaseUtil.checkTableExists(connection, tableName);
        
        assertTrue(result);
    }

    // [] Test: valid table name with underscores
    @Test
    public void testCheckTableExists_ValidTableName_WithUnderscores() throws SQLException {
        String tableName = "test_table_name";
        
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getInt(1)).thenReturn(1);

        boolean result = KingbaseUtil.checkTableExists(connection, tableName);
        
        assertTrue(result);
    }

    // [] Test: valid table name with all allowed characters
    @Test
    public void testCheckTableExists_ValidTableName_AllAllowedCharacters() throws SQLException {
        String tableName = "test_123_table";
        
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getInt(1)).thenReturn(1);

        boolean result = KingbaseUtil.checkTableExists(connection, tableName);
        
        assertTrue(result);
    }

    // [] Test: invalid table name with hyphen
    @Test
    public void testCheckTableExists_InvalidTableName_WithHyphen() throws SQLException {
        String tableName = "test-table";

        DataproxyException exception = assertThrows(DataproxyException.class, () -> {
            KingbaseUtil.checkTableExists(connection, tableName);
        });
        
        assertEquals(DataproxyErrorCode.PARAMS_UNRELIABLE, exception.getErrorCode());
        assertTrue(exception.getMessage().contains("Invalid tableName:test-table"));
        
        verify(connection, never()).createStatement();
    }

    // [] Test: invalid table name with spaces
    @Test
    public void testCheckTableExists_InvalidTableName_WithSpaces() throws SQLException {
        String tableName = "test table";

        DataproxyException exception = assertThrows(DataproxyException.class, () -> {
            KingbaseUtil.checkTableExists(connection, tableName);
        });
        
        assertEquals(DataproxyErrorCode.PARAMS_UNRELIABLE, exception.getErrorCode());
        assertTrue(exception.getMessage().contains("Invalid tableName:test table"));
        
        verify(connection, never()).createStatement();
    }

    // [] Test: invalid table name with special characters
    @Test
    public void testCheckTableExists_InvalidTableName_WithSpecialChars() throws SQLException {
        String tableName = "table@name";

        DataproxyException exception = assertThrows(DataproxyException.class, () -> {
            KingbaseUtil.checkTableExists(connection, tableName);
        });
        
        assertEquals(DataproxyErrorCode.PARAMS_UNRELIABLE, exception.getErrorCode());
        assertTrue(exception.getMessage().contains("Invalid tableName:table@name"));
        
        verify(connection, never()).createStatement();
    }

    // [] Test: invalid table name with dot
    @Test
    public void testCheckTableExists_InvalidTableName_WithDot() throws SQLException {
        String tableName = "schema.table.1";

        DataproxyException exception = assertThrows(DataproxyException.class, () -> {
            KingbaseUtil.checkTableExists(connection, tableName);
        });
        
        assertEquals(DataproxyErrorCode.PARAMS_UNRELIABLE, exception.getErrorCode());
        assertTrue(exception.getMessage().contains("Invalid tableName:schema.table"));
        
        verify(connection, never()).createStatement();
    }

    // [] Test: invalid table name starting with number
    @Test
    public void testCheckTableExists_InvalidTableName_StartingWithNumber() throws SQLException {
        String tableName = "123table";

        // Note: This is actually VALID since the pattern is ^[a-zA-Z0-9_]+$
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getInt(1)).thenReturn(1);

        boolean result = KingbaseUtil.checkTableExists(connection, tableName);
        
        assertTrue(result);
    }

    // [] Test: empty table name
    @Test
    public void testCheckTableExists_EmptyTableName() throws SQLException {
        String tableName = "";

        DataproxyException exception = assertThrows(DataproxyException.class, () -> {
            KingbaseUtil.checkTableExists(connection, tableName);
        });
        
        assertEquals(DataproxyErrorCode.PARAMS_UNRELIABLE, exception.getErrorCode());
        assertTrue(exception.getMessage().contains("Invalid tableName:"));
        
        verify(connection, never()).createStatement();
    }

    // [] Test: table name with single underscore
    @Test
    public void testCheckTableExists_SingleUnderscore() throws SQLException {
        String tableName = "_";
        
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getInt(1)).thenReturn(1);

        boolean result = KingbaseUtil.checkTableExists(connection, tableName);
        
        assertTrue(result);
    }

    // [] Test: table exists with count > 1 (multiple matches)
    @Test
    public void testCheckTableExists_TableExistsMultipleMatches() throws SQLException {
        String tableName = "test_table";
        
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getInt(1)).thenReturn(5); // Multiple tables found

        boolean result = KingbaseUtil.checkTableExists(connection, tableName);
        
        assertTrue(result);
    }

    // [] Test: verify SQL query uses correct table name
    @Test
    public void testCheckTableExists_VerifySqlQuery() throws SQLException {
        String tableName = "my_test_table";
        
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getInt(1)).thenReturn(1);

        KingbaseUtil.checkTableExists(connection, tableName);
        
        verify(statement).executeQuery(eq("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'my_test_table' AND table_schema NOT IN ('pg_catalog', 'information_schema')"));
    }
}
