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

package org.secretflow.dataproxy.plugin.kingbase.converter;

import org.secretflow.dataproxy.plugin.database.config.DatabaseTableConfig;
import org.secretflow.dataproxy.plugin.database.config.DatabaseTableQueryConfig;
import org.secretflow.dataproxy.plugin.database.config.DatabaseWriteConfig;
import org.secretflow.dataproxy.plugin.database.converter.DatabaseParamConverter;
import org.secretflow.dataproxy.plugin.kingbase.config.KingbaseDatabaseWriteConfig;
import org.secretflow.dataproxy.plugin.kingbase.config.KingbaseDatatableCommandConfig;
import org.secretflow.v1alpha1.kusciaapi.Domaindata;
import org.secretflow.v1alpha1.kusciaapi.Domaindatasource;
import org.secretflow.v1alpha1.kusciaapi.Flightinner;

/**
 *
 * @author chenmingliang
 * @date 2025/12/12
 */
public class KingbaseDatabaseParamConverter extends DatabaseParamConverter {

    @Override
    public DatabaseTableQueryConfig convert(Flightinner.CommandDataMeshQuery request) {
        Domaindatasource.DatabaseDataSourceInfo db = request.getDatasource().getInfo().getDatabase();
        Domaindata.DomainData domaindata = request.getDomaindata();

        String tableName = domaindata.getRelativeUri();
        String partitionSpec = request.getQuery().getPartitionSpec();
        DatabaseTableConfig dbTableConfig = new DatabaseTableConfig(tableName, partitionSpec, domaindata.getColumnsList());
        // generateDatabaseTableQueryConfig()
        return new KingbaseDatatableCommandConfig(convert(db), dbTableConfig);
    }

    @Override
    public DatabaseWriteConfig convert(Flightinner.CommandDataMeshUpdate request) {
        Domaindatasource.DatabaseDataSourceInfo db = request.getDatasource().getInfo().getDatabase();
        Domaindata.DomainData domainData = request.getDomaindata();
        String tableName = domainData.getRelativeUri();
        String partitionSpec = request.getUpdate().getPartitionSpec();
        DatabaseTableConfig dbtableConfig = new DatabaseTableConfig(tableName, partitionSpec, domainData.getColumnsList());
        return new KingbaseDatabaseWriteConfig(convert(db), dbtableConfig);

    }
}
