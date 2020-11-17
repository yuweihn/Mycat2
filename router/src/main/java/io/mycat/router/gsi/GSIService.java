package io.mycat.router.gsi;

import com.alibaba.fastsql.sql.ast.SQLStatement;
import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 *  全局二级索引 (Global Secondary Index, GSI)
 *
 *  create global index index_name on table_name(name...)
 *  ---------------------------------------------------------------
 *  | 数据源 | 主键列 | 索引列(支持多个) |  状态                       |
 *  --------------------------------------------------------------|
 *  |       | id    | name          | unCommit,preCommit,commit   |
 *  --------------------------------------------------------------|
 *  | db1   |  1    | 小王          |                              |
 *  | db2   |  2    | 小李          |                              |
 *  ---------------------------------------------------------------
 *
 *  关于全局索引与数据库的一致性.
 *  -----------------------------------------------------------
 *  |角色|    操作                                              |
 *  -----------------------------------------------------------
 *  |gsi | insert     |        |  preCommit |        |  commit |
 *  -----------------------------------------------------------
 *  |db  | 关闭自动提交 | insert |            | commit |         |
 *  -----------------------------------------------------------
 *
 *  1. GSI启动时, 会检查状态
 *      如果状态小于 preCommit, 直接回滚.
 *      如果状态等于 preCommit, 则到db中同步当前行数据.
 *
 * -----------------------------------------------
 *
 * 存储接口 io.mycat.metadata.CustomTableHandler
 *   mapdb
 *  Chronicle-Map
 *  apache ignite
 *
 * @author wangzihaogithub 2020年11月8日17:53:57
 */
public interface GSIService {
    int drop(String tableName, String indexName);
    int create(String tableName, String indexName, String[] pkColumnNames, String[] indexColumnNames);

    Transaction insert(String tableName, IndexRowData rowData);
    Transaction updateByIndex(String tableName, IndexRowData rowData, Map<String, Object> index);
    Transaction updateByPk(String tableName, IndexRowData rowData, Map<String, Object> pk);
    Transaction deleteByIndex(String tableName, Map<String, Object> index);
    Transaction deleteByPk(String tableName, Map<String, Object> pk);

    boolean preCommit(Long txId);
    boolean commit(Long txId);
    boolean rollback(Long txId);

    List<IndexRowData> select(SQLStatement statement);

    @Data
    class Transaction{
        private Long id;
    }

    @Data
    class IndexRowData{
        private String datasourceKey;
        private Map<String,Object> pkColumnValues;
        private Map<String,Object> indexColumnValues;
    }

}
