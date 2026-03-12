/*
 * Copyright 2025 Ant Group Co., Ltd.
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

package org.secretflow.dataproxy.plugin.database.converter;

import lombok.extern.slf4j.Slf4j;
import org.secretflow.dataproxy.plugin.database.config.*;
import org.secretflow.dataproxy.core.converter.DataProxyParamConverter;
import org.secretflow.v1alpha1.kusciaapi.Domaindata;
import org.secretflow.v1alpha1.kusciaapi.Domaindatasource;
import org.secretflow.v1alpha1.kusciaapi.Flightinner;

@Slf4j
public class DatabaseParamConverter implements DataProxyParamConverter<ScqlCommandJobConfig, DatabaseTableQueryConfig, DatabaseWriteConfig> {

    @Override
    public ScqlCommandJobConfig convert(Flightinner.CommandDataMeshSqlQuery request) {
        Domaindatasource.DatabaseDataSourceInfo db = request.getDatasource().getInfo().getDatabase();
        String sql = request.getQuery().getSql();
        
        validateSqlSafety(sql);
        
        return new ScqlCommandJobConfig(convert(db), sql);
    }

   
    private void validateSqlSafety(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            throw new IllegalArgumentException("SQL query cannot be null or empty");
        }

        String upperSql = sql.trim().toUpperCase();

        if (!upperSql.startsWith("SELECT")) {
            throw new IllegalArgumentException("Only SELECT queries are allowed. Query starts with: " + 
                upperSql.substring(0, Math.min(20, upperSql.length())));
        }

        String[] forbiddenKeywords = {
            "DROP", "DELETE", "INSERT", "UPDATE", "TRUNCATE",
            "CREATE", "ALTER", "RENAME", "GRANT", "REVOKE",
            "EXECUTE", "EXEC", "CALL", "DECLARE"
        };

        for (String keyword : forbiddenKeywords) {
            int index = upperSql.indexOf(keyword);
            if (index != -1) {
                if (!isKeywordInCommentOrString(sql, index)) {
                    throw new IllegalArgumentException(
                        "SQL query contains forbidden keyword '" + keyword + "'. " +
                        "Only SELECT operations are allowed for security reasons.");
                }
            }
        }

        String[] injectionPatterns = {
            ";--", ";#", "\\x00", "%00",
            "1=1", "1 = 1", "OR TRUE", "AND TRUE"
        };

        String upperSqlForInjection = upperSql.replaceAll("\\s+", " ");
        for (String pattern : injectionPatterns) {
            if (upperSqlForInjection.contains(pattern)) {
                throw new IllegalArgumentException(
                    "SQL query contains potential injection pattern: " + pattern);
            }
        }

        if (upperSql.contains("UNION")) {
            int unionIndex = upperSqlForInjection.indexOf("UNION");
            int nextNonSpace = unionIndex + 5;
            if (nextNonSpace < upperSqlForInjection.length()) {
                String nextPart = upperSqlForInjection.substring(nextNonSpace, 
                    Math.min(nextNonSpace + 10, upperSqlForInjection.length()));
                if (nextPart.trim().startsWith("SELECT")) {
                    log.warn("SQL query contains UNION SELECT, please ensure this is intentional: {}",
                        sql.length() > 200 ? sql.substring(0, 200) + "..." : sql);
                }
            }
        }

        log.debug("SQL query validated successfully: {}", sql.length() > 200 ? sql.substring(0, 200) + "..." : sql);
    }


    private boolean isKeywordInCommentOrString(String sql, int keywordIndex) {
        int start = Math.max(0, keywordIndex - 50);
        int end = Math.min(sql.length(), keywordIndex + 50);
        String context = sql.substring(start, end).toLowerCase();
        
        int quoteCount = context.split("'", -1).length - 1;
        if (quoteCount % 2 != 0) {
            return true;
        }
        
        int commentStart = context.indexOf("--");
        if (commentStart != -1 && commentStart < (keywordIndex - start)) {
            return true;
        }
        commentStart = context.indexOf("#");
        if (commentStart != -1 && commentStart < (keywordIndex - start)) {
            return true;
        }
        commentStart = context.indexOf("/*");
        if (commentStart != -1 && commentStart < (keywordIndex - start)) {
            if (context.indexOf("*/") > (keywordIndex - start)) {
                return true;
            }
        }
        
        return false;
    }

    @Override
    public DatabaseTableQueryConfig convert(Flightinner.CommandDataMeshQuery request) {
        Domaindatasource.DatabaseDataSourceInfo db = request.getDatasource().getInfo().getDatabase();
        Domaindata.DomainData domaindata = request.getDomaindata();

        String tableName = domaindata.getRelativeUri();
        String partitionSpec = request.getQuery().getPartitionSpec();
        DatabaseTableConfig dbTableConfig = new DatabaseTableConfig(tableName, partitionSpec, domaindata.getColumnsList());
        return new DatabaseTableQueryConfig(convert(db), dbTableConfig);
    }

    @Override
    public DatabaseWriteConfig convert(Flightinner.CommandDataMeshUpdate request) {
        Domaindatasource.DatabaseDataSourceInfo db = request.getDatasource().getInfo().getDatabase();
        Domaindata.DomainData domainData = request.getDomaindata();
        String tableName = domainData.getRelativeUri();
        String partitionSpec = request.getUpdate().getPartitionSpec();
        DatabaseTableConfig dbtableConfig = new DatabaseTableConfig(tableName, partitionSpec, domainData.getColumnsList());
        return new DatabaseWriteConfig(convert(db), dbtableConfig);

    }

    protected static DatabaseConnectConfig convert(Domaindatasource.DatabaseDataSourceInfo db) {
        return new DatabaseConnectConfig(db.getUser(), db.getPassword(), db.getEndpoint(), db.getDatabase());
    }
}
