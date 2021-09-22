package io.mycat.config;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.HashMap;
import java.util.Map;

@Data
@Builder
@EqualsAndHashCode
public class ShardingTableConfig {
    ShardingBackEndTableInfoConfig partition = new ShardingBackEndTableInfoConfig();
    ShardingFunction function = new ShardingFunction();
    String createTableSQL;
    Boolean autoIncrement;

    Map<String,ShardingTableConfig> shardingIndexTables = new HashMap<>();

    public ShardingTableConfig(ShardingBackEndTableInfoConfig partition, ShardingFunction function, String createTableSQL,Boolean autoIncrement, Map<String, ShardingTableConfig> shardingIndexTables) {
        this.partition = partition;
        this.function = function;
        this.createTableSQL = createTableSQL;
        this.autoIncrement = autoIncrement;
        this.shardingIndexTables = shardingIndexTables;
    }

    public ShardingTableConfig() {
    }

    public void setShardingIndexTables(Map<String, ShardingTableConfig> shardingIndexTables) {
        this.shardingIndexTables = shardingIndexTables;
    }
}