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

package org.secretflow.dataproxy.plugin.kingbase.config;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.secretflow.dataproxy.plugin.database.config.DatabaseConnectConfig;
import org.secretflow.dataproxy.plugin.database.config.DatabaseTableConfig;
import org.secretflow.dataproxy.plugin.database.config.DatabaseTableQueryConfig;
import org.secretflow.dataproxy.plugin.kingbase.utils.KingbaseDbTypeToKusciaTypeUtil;

import java.util.stream.Collectors;

/**
 *
 * @author chenmingliang
 * @date 2025/12/12
 */
public class KingbaseDatatableCommandConfig extends DatabaseTableQueryConfig {
    public KingbaseDatatableCommandConfig(DatabaseConnectConfig dbConnectConfig, DatabaseTableConfig readConfig) {
        super(dbConnectConfig, readConfig);
    }

    @Override
    public Schema getResultSchema() {

        return new Schema(commandConfig.columns().stream()
                .map(column ->
                        Field.nullable(column.getName(), KingbaseDbTypeToKusciaTypeUtil.parseKusciaColumnType(column.getType())))
                .collect(Collectors.toList()));
    }
}
