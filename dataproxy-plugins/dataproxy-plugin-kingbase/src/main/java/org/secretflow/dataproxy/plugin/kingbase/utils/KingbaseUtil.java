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

import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.secretflow.dataproxy.common.exceptions.DataproxyErrorCode;
import org.secretflow.dataproxy.common.exceptions.DataproxyException;
import org.secretflow.dataproxy.plugin.database.config.DatabaseConnectConfig;
import org.secretflow.dataproxy.plugin.database.writer.DatabaseRecordWriter;
import org.secretflow.dataproxy.plugin.database.writer.DatabaseRecordWriter.SqlWithParams;

import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.secretflow.dataproxy.plugin.database.writer.DatabaseRecordWriter.parsePartition;

/**
 *
 * @author chenmingliang
 * @date 2025/12/09
 */
@Slf4j
public class KingbaseUtil {
    public static Connection initKingbase(DatabaseConnectConfig config) {
        String endpoint = config.endpoint();
        String ip;
        // Kingbase default port
        int port = 54321;

        if (endpoint.contains(":")) {
            String[] parts = endpoint.split(":");
            ip = parts[0];
            if (parts.length > 1 && !parts[1].isEmpty()) {
                port = Integer.parseInt(parts[1]);
            }
        } else {
            ip = endpoint;
        }

        // Validate IP address/hostname to prevent JDBC URL injection
        if (ip == null || !ip.matches("^[a-zA-Z0-9._-]+$") || ip.contains("..") || ip.startsWith(".") || ip.endsWith(".")) {
            throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE,
                    "Invalid IP address or hostname: " + ip);
        }

        // Validate database name to prevent JDBC URL injection
        String database = config.database();
        if (database == null || !database.matches("^[a-zA-Z0-9_]+$")) {
            throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE,
                    "Invalid database name: " + database);
        }

        Connection conn;
        try {
            // Load Kingbase JDBC driver
            Class.forName("com.kingbase8.Driver");

            // Build Kingbase JDBC URL
            String jdbcUrl = String.format("jdbc:kingbase8://%s:%d/%s", ip, port, database);

            // Create connection
            if (!config.username().isEmpty() && !config.password().isEmpty()) {
                conn = DriverManager.getConnection(jdbcUrl, config.username(), config.password());
            } else {
                conn = DriverManager.getConnection(jdbcUrl);
            }

            log.info("Successfully connected to Kingbase database: {}", jdbcUrl);
            conn.setAutoCommit(true);
            return conn;
        } catch (Exception e) {
            log.error("Kingbase database init error: {}", e.getMessage());
            throw new RuntimeException("Failed to connect to Kingbase database", e);
        }
    }

    public static SqlWithParams buildQuerySql(String tableName, List<String> fields, String whereClause) {
        final Pattern identifierPattern = Pattern.compile("^[a-zA-Z0-9_]+$");
        final Pattern identifierPatternWithDot = Pattern.compile("^[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+$");

        // Validate table name (support both simple name and schema.table format)
        if (!identifierPattern.matcher(tableName).matches() && !identifierPatternWithDot.matcher(tableName).matches()) {
            throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid tableName:" + tableName);
        }

        // Validate field names
        for (String field : fields) {
            if (!identifierPattern.matcher(field).matches()) {
                throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid field name:" + field);
            }
        }

        // Process where clause (similar to Hive partition handling)
        List<Object> whereParams = new ArrayList<>();
        String processedWhereClause = "";
        if (whereClause != null && !whereClause.trim().isEmpty()) {
            final Map<String, String> partitionSpec = parsePartition(whereClause);
            
            if (!partitionSpec.isEmpty()) {
                List<String> list = new ArrayList<>();
                for (Map.Entry<String, String> entry : partitionSpec.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();

                    // Validate partition key name
                    if (!identifierPattern.matcher(key).matches()) {
                        throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid partition key:" + key);
                    }

                    // Validate partition value
                    if (!value.matches("^[a-zA-Z0-9_.-]+$")) {
                        throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid partition value:" + value);
                    }

                    // Use parameter placeholder
                    list.add(key + " = ?");
                    whereParams.add(value);
                }
                processedWhereClause = String.join(" and ", list);
            } else {
                // For simple where conditions, reject as unsafe
                throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE,
                        "Invalid where clause format. Use partition format like 'key=value,key2=value2'");
            }
        }

        String sql = "SELECT " + String.join(", ", fields) + " FROM " + tableName +
                (processedWhereClause.isEmpty() ? "" : " WHERE " + processedWhereClause);
        log.info("buildQuerySql sql:{}", sql);
        return new SqlWithParams(sql, whereParams);
    }

    public static String buildCreateTableSql(String tableName, Schema schema, Map<String, String> partition) {
        final Pattern identifierPattern = Pattern.compile("^[a-zA-Z0-9_]+$");
        final Pattern identifierPatternWithDot = Pattern.compile("^[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+$");

        // Validate table name (support both simple name and schema.table format)
        if (!identifierPattern.matcher(tableName).matches() && !identifierPatternWithDot.matcher(tableName).matches()) {
            throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid tableName:" + tableName);
        }

        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(tableName).append(" (\n");

        List<Field> fields = schema.getFields();

        // Used for quick field lookup by name
        Map<String, Field> fieldMap = new LinkedHashMap<>();
        for (Field field : fields) {
            fieldMap.put(field.getName(), field);
        }

        // Validate all field names
        for (Field field : fields) {
            String fieldName = field.getName();
            if (!identifierPattern.matcher(fieldName).matches()) {
                throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid field name:" + fieldName);
            }
        }

        // Table fields (excluding partition fields)
        boolean first = true;
        for (Field field : fields) {
            String fieldName = field.getName();

            if (!first) {
                sb.append(",\n");
            }
            sb.append("  ").append(fieldName)
                    .append(" ")
                    .append(arrowTypeToJdbcType(field.getType()));
            first = false;
        }
        sb.append("\n)");


        log.info("buildCreateTableSql sql:{}", sb);
        return sb.toString();
    }

    public static DatabaseRecordWriter.SqlWithParams buildMultiRowInsertSql(String tableName,
                                                                            Schema schema,
                                                                            List<Map<String, Object>> dataList,
                                                                            Map<String, String> partition) {
        final Pattern identifierPattern = Pattern.compile("^[a-zA-Z0-9_]+$");
        final Pattern identifierPatternWithDot = Pattern.compile("^[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+$");

        if (dataList == null || dataList.isEmpty()) {
            throw new IllegalArgumentException("No data to insert");
        }

        // Validate table name (support both simple name and schema.table format)
        if (!identifierPattern.matcher(tableName).matches() && !identifierPatternWithDot.matcher(tableName).matches()) {
            throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid tableName:" + tableName);
        }
        for (Field f : schema.getFields()) {
            if (!identifierPattern.matcher(f.getName()).matches()) {
                throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid field name:" + f.getName());
            }
        }
        List<String>columns = new ArrayList<>();
        if (partition != null && !partition.isEmpty()) {
            for (String k : partition.keySet()) {
                if (!identifierPattern.matcher(k).matches()) {
                    throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid partition key:" + k);
                }
            }
            Set<String> partitionKeys = partition.keySet();
            columns = schema.getFields().stream()
                    .map(Field::getName)
                    .filter(n -> !partitionKeys.contains(n))
                    .collect(Collectors.toList());
        } else {
            columns = schema.getFields().stream().map(Field::getName).collect(Collectors.toList());
        }



        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(tableName);

        // Kingbase doesn't use Hive's PARTITION syntax for inserts
        sb.append(" (").append(String.join(", ", columns)).append(") VALUES ");

        String singleRow = "(" + String.join(", ", Collections.nCopies(columns.size(), "?")) + ")";
        List<String> allRows = Collections.nCopies(dataList.size(), singleRow);
        sb.append(String.join(", ", allRows));

        List<Object> params = new ArrayList<>();

        for (Map<String, Object> row : dataList) {
            for (String col : columns) {
                params.add(row.get(col));
            }
        }

        // Handle partition values if needed
        if (partition != null && !partition.isEmpty()) {
            // For partitioned tables, we might need to handle partition values differently
            // This is a simplified approach - actual implementation depends on Kingbase partition strategy
            Set<String> partitionKeys = partition.keySet();
            for (String partKey : partitionKeys) {
                for (Map<String, Object> row : dataList) {
                    if (row.containsKey(partKey)) {
                        params.add(row.get(partKey));
                    }
                }
            }
        }

        log.info("buildMultiRowInsertSql sql:{}", sb);
        log.info("buildMultiRowInsertSql params:{}", params);
        return new DatabaseRecordWriter.SqlWithParams(sb.toString(), params);
    }

    public static String escapeString(String str) {
        return str.replace("'", "''");
    }

    public static ArrowType jdbcType2ArrowType(String jdbcType) {
        if (jdbcType == null || jdbcType.isEmpty()) {
            throw new IllegalArgumentException("Kingbase type is null or empty");
        }

        String type = jdbcType.trim().toLowerCase();

        // Handle decimal(p,s)
        if (type.startsWith("decimal")) {
            Pattern pattern = Pattern.compile("decimal\\((\\d+),(\\d+)\\)");
            Matcher matcher = pattern.matcher(type);
            if (matcher.find()) {
                int precision = Integer.parseInt(matcher.group(1));
                int scale = Integer.parseInt(matcher.group(2));
                return new ArrowType.Decimal(precision, scale, 128);
            } else {
                // Default precision
                return new ArrowType.Decimal(38, 10, 128);
            }
        }

        return switch (type) {
            case "smallint", "int2" -> new ArrowType.Int(16, true);
            case "integer", "int", "int4" , "smallserial", "serial2", "serial", "serial4", "bigserial", "serial8"-> new ArrowType.Int(32, true);
            case "bigint", "int8" -> new ArrowType.Int(64, true);
            case "real", "float4" -> new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case "double precision", "float8", "numeric" -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            //kingbase "character varying" equals "varchar"
            case "character varying", "varchar", "text", "char", "enum", "json" -> new ArrowType.Utf8();
            case "boolean", "bool" -> new ArrowType.Bool();
            case "date" -> new ArrowType.Date(DateUnit.DAY);
            //timestamp[p] not supported now
            case "timestamp"-> new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
            case "timestamptz" -> new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC");
            //time[p] not supported now
            case "time" -> new ArrowType.Time(TimeUnit.MILLISECOND, 32);
            case "bytea" -> new ArrowType.Binary();
            // oid is an object identifier type in PostgreSQL/Kingbase
            case "oid" -> new ArrowType.Int(32, true);

            default -> throw new IllegalArgumentException("Unsupported Kingbase type: " + jdbcType);
        };
    }

    public static String arrowTypeToJdbcType(ArrowType arrowType) {
        if (arrowType instanceof ArrowType.Utf8) {
            return "VARCHAR";
        } else if (arrowType instanceof ArrowType.Int intType) {
            int bitWidth = intType.getBitWidth();
            boolean signed = intType.getIsSigned();
            return switch (bitWidth) {
                // Kingbase doesn't have tinyint
                case 8 -> signed ? "SMALLINT" : "SMALLINT";
                case 16 -> signed ? "SMALLINT" : "SMALLINT";
                case 32 -> signed ? "INTEGER" : "INTEGER";
                case 64 -> signed ? "BIGINT" : "BIGINT";
                default -> throw new IllegalArgumentException("Unsupported Int bitWidth: " + bitWidth);
            };
        } else if (arrowType instanceof ArrowType.FloatingPoint fp) {
            return switch (fp.getPrecision()) {
                case SINGLE -> "REAL";
                case DOUBLE -> "DOUBLE PRECISION";
                default -> throw new IllegalArgumentException("Unsupported floating point type");
            };
        } else if (arrowType instanceof ArrowType.Bool) {
            return "BOOLEAN";
        } else if (arrowType instanceof ArrowType.Date) {
            return "DATE";
        } else if (arrowType instanceof ArrowType.Time) {
            return "TIME";
        } else if (arrowType instanceof ArrowType.Timestamp) {
            return "TIMESTAMP";
        } else if (arrowType instanceof ArrowType.Decimal dec) {
            return "DECIMAL(" + dec.getPrecision() + ", " + dec.getScale() + ")";
        } else if (arrowType instanceof ArrowType.Binary || arrowType instanceof ArrowType.FixedSizeBinary) {
            return "BYTEA";
        } else {
            throw new IllegalArgumentException("Unsupported Arrow type: " + arrowType.getClass());
        }
    }

    public static boolean checkTableExists(Connection connection, String tableName) {
        final Pattern identifierPattern = Pattern.compile("^[a-zA-Z0-9_]+$");
        final Pattern identifierPatternWithDot = Pattern.compile("^[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+$");

        // Validate table name (support both simple name and schema.table format)
        if (!identifierPattern.matcher(tableName).matches() && !identifierPatternWithDot.matcher(tableName).matches()) {
            throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid tableName:" + tableName);
        }

        ResultSet rs = null;
        Statement stmt = null;
        try {
            stmt = connection.createStatement();

            // Check if table name contains schema separator
            if (tableName.contains(".")) {
                String[] parts = tableName.split("\\.");
                String schemaName = parts[0];
                String actualTableName = parts[1];

                // Use Kingbase system catalog to check table existence with schema
                rs = stmt.executeQuery(
                        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '" + schemaName + "' AND table_name = '" + actualTableName + "'"
                );
            } else {
                // Use Kingbase system catalog to check table existence (current user's schema)
                rs = stmt.executeQuery(
                        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '" + tableName + "' AND table_schema NOT IN ('pg_catalog', 'information_schema')"
                );
            }
            return rs.next() && rs.getInt(1) > 0;
        } catch (SQLException e) {
            log.error("check whether table has existed error: {}", e.getMessage());
            throw new RuntimeException(e);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (SQLException e) {
                log.error("close result set error: {}", e.getMessage());
                throw new RuntimeException(e);
            }
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
                log.error("close statement error: {}", e.getMessage());
                throw new RuntimeException(e);
            }
        }
    }
}
