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
