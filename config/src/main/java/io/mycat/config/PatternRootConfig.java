package io.mycat.config;

import io.mycat.util.YamlUtil;
import lombok.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Data
@Builder
@EqualsAndHashCode
public class PatternRootConfig {
    private UserConfig user;
    private List<Map<String,Object>> sqls = new ArrayList<>();
    private List<List<Map<String,Object>>> sqlsGroup = new ArrayList<>();
    private Map<String,Object> defaultHanlder;
    private String transactionType;
    private String matcherClazz;
    private List<String> boosters = new ArrayList<>();

    public PatternRootConfig() {
    }

    public PatternRootConfig(UserConfig user, List<Map<String, Object>> sqls, List<List<Map<String, Object>>> sqlsGroup, Map<String, Object> defaultHanlder, String transactionType, String matcherClazz,List<String> boosters) {
        this.user = user;
        this.sqls = sqls;
        this.sqlsGroup = sqlsGroup;
        this.defaultHanlder = defaultHanlder;
        this.transactionType = transactionType;
        this.matcherClazz = matcherClazz;
        this.boosters = boosters;
    }

    public List<Map<String,Object>> getSqls() {//注意去重
        if (sqlsGroup == null){
            sqlsGroup = Collections.emptyList();
        }
        if (sqls == null){
            sqls = Collections.emptyList();
        }
        return Stream.concat(sqlsGroup.stream().flatMap(i -> i.stream()), sqls.stream()).distinct().collect(Collectors.toList());
    }



    public static void main(String[] args) {
        PatternRootConfig config = PatternRootConfig.builder().user(
                UserConfig.builder()
                        .ip(".")
                        .password("123456")
                        .username("root")
                        .build()
        )
                .build();
        String dump = YamlUtil.dump(config);
        System.out.println(dump);
    }
}