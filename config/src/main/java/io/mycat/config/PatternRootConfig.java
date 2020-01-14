package io.mycat.config;

import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class PatternRootConfig {
    private List<SchemaConfig> schemas = new ArrayList<>();
    private List<TextItemConfig> sqls = new ArrayList<>();
//    private List<HandlerToSQLs> handlers = new ArrayList<>();
    private Handler defaultHanlder;
    private String transactionType;
    private String defaultSchema;


    @Data
    public static class HandlerToSQLs {
        String name;
        List<String> tables = new ArrayList<>();
        List<String> sqls = new ArrayList<>();
        List<String> hints = new ArrayList<>();
        Map<String, String> tags;
        String type;
        String explain;
    }


    @Data

    public static class TextItemConfig {
        String name;
        String sql;
        //handler
        List<String> hints = new ArrayList<>();
        Map<String, String> tags = new HashMap<>();
        String command;
        String explain;
//        String transactionType;
    }


    @Data
    public static class Handler {
         List<String> hints = new ArrayList<>();
         Map<String, String> tags;
         String command;
         String explain;
//         String transactionType;
    }


    @Data
    public static class SchemaConfig {
        String name;
        List<String> tables = new ArrayList<>();
        List<TextItemConfig> sqls = new ArrayList<>();
        private Handler defaultHanlder;
    }

    public static void main(String[] args) {
    }
}