package io.mycat.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Data
public class PatternRootConfig {
    private UserConfig user;
    private List<SchemaConfig> schemas = new ArrayList<>();
    private List<TextItemConfig> sqls = new ArrayList<>();
    private List<List<TextItemConfig>> sqlsGroup = new ArrayList<>();
    private Handler defaultHanlder;
    private String transactionType;

    public List<TextItemConfig> getSqls() {//注意去重
        return Stream.concat(sqlsGroup.stream().flatMap(i -> i.stream()), sqls.stream()).distinct().collect(Collectors.toList());
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class UserConfig {
        private String username;
        private String password;
        private String ip;
    }

    @Data
    @ToString
    public static class TextItemConfig {
        String name;
        String sql;
        //handler
        List<String> hints = new ArrayList<>();
        Map<String, String> tags = new HashMap<>();
        String command;
        String explain;
        String cache;
        Boolean simply;
//        String transactionType;
    }


    @Data
    public static class Handler {
        List<String> hints = new ArrayList<>();
        Map<String, String> tags;
        String command;
        String explain;
        String cache;
        Boolean simply;
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