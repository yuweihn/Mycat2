package io.mycat.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.*;

@Data
public class ShardingQueryRootConfig {
    Map<String, LogicSchemaConfig> schemas = new HashMap<>();
    PrototypeServer prototype;

    @AllArgsConstructor
    @Data
    @Builder
    public static class LogicTableConfig {
        List<BackEndTableInfoConfig> dataNodes;
        List<Column> columns = new ArrayList<>();
        String createTableSQL;

        public LogicTableConfig() {
        }
    }

    @AllArgsConstructor
    @Data
    @Builder
    public static class BackEndTableInfoConfig {
        private String targetName;
        private String schemaName;
        private String tableName;

        public BackEndTableInfoConfig() {
        }
    }


    @Data
    public static final class LogicSchemaConfig {
        Map<String,LogicTableConfig> tables = new HashMap<>();
    }

    @AllArgsConstructor
    @Data
    @Builder
    public static final class Column {
        String columnName;
        SharingFuntionRootConfig.ShardingFuntion function;
         String shardingType;
         List<String> map;

        public Column() {
        }

        public List<String> getMap() {
            return map == null? Collections.emptyList():map;
        }
    }

    @Data
    @Builder
    public static class PrototypeServer{
        String url;
        String user;
        String password;
    }

}