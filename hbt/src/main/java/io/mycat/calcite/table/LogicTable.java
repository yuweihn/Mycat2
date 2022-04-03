/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mycat.calcite.table;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateViewStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import io.mycat.*;
import io.mycat.config.GlobalTableConfig;
import io.mycat.config.NormalTableConfig;
import io.mycat.config.ShardingTableConfig;
import io.mycat.router.CustomRuleFunction;
import io.mycat.util.MycatSQLExprTableSourceUtil;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

@Getter
public class LogicTable {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogicTable.class);
    private final String uniqueName;
    private final LogicTableType type;
    private final String schemaName;
    private final String tableName;
    private final List<SimpleColumnInfo> rawColumns;
    private final String createTableSQL;
    private final SimpleColumnInfo autoIncrementColumn;
    private final Map<String, IndexInfo> indexes;

    //优化,非必须
    private final Map<String, SimpleColumnInfo> map;


    public LogicTable(LogicTableType type, String schemaName,
                      String tableName,
                      List<SimpleColumnInfo> rawColumns,
                      Map<String, IndexInfo> indexInfos,
                      String createTableSQL) {
        /////////////////////////////////////////
        schemaName = SQLUtils.normalize(schemaName).toLowerCase();
        tableName = SQLUtils.normalize(tableName).toLowerCase();
        this.uniqueName = schemaName + "_" + tableName;
        this.type = type;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.rawColumns = rawColumns;
        this.indexes = indexInfos;
        this.createTableSQL = normalizeCreateTableSQL(schemaName, createTableSQL);
        /////////////////////////////////////////
        this.autoIncrementColumn = rawColumns.stream().filter(i -> i.isAutoIncrement()).findFirst().orElse(null);
        /////////////////////////////////////////
        Map<String, SimpleColumnInfo> result = new HashMap<>();
        for (SimpleColumnInfo k : rawColumns) {
            result.put(k.getColumnName(), k);
        }
        this.map = result;
    }

    private static String normalizeCreateTableSQL(String schemaName, String createTableSQL) {
        try {
            SQLStatement createTableAst = SQLUtils.parseSingleMysqlStatement(createTableSQL);
            if (createTableAst instanceof SQLCreateTableStatement) {
                ((SQLCreateTableStatement) createTableAst).setIfNotExiists(true);
                ((SQLCreateTableStatement) createTableAst).setSchema("`" + schemaName + "`");
            }
            if (createTableAst instanceof MySqlCreateTableStatement) {
                ((MySqlCreateTableStatement) createTableAst).setIfNotExiists(true);
                ((MySqlCreateTableStatement) createTableAst).setSchema("`" + schemaName + "`");
            }
            if (createTableAst instanceof SQLCreateViewStatement) {
                ((SQLCreateViewStatement) createTableAst).setIfNotExists(true);
                SQLExprTableSource tableSource = ((SQLCreateViewStatement) createTableAst).getTableSource();
                MycatSQLExprTableSourceUtil.setSqlExprTableSource(schemaName, tableSource.getTableName(), tableSource);
            }
            return Objects.requireNonNull(SQLUtils.toMySqlString(createTableAst));
        } catch (Throwable throwable) {
            LOGGER.error("", throwable);
        }
        return createTableSQL;
    }

    public static TableHandler createGlobalTable(String schemaName,
                                                 String tableName,
                                                 List<Partition> backendTableInfos,
                                                 List<SimpleColumnInfo> columns,
                                                 Map<String, IndexInfo> indexInfos,
                                                 String createTableSQL,
                                                 GlobalTableConfig tableConfigEntry) {
        LogicTable logicTable = new LogicTable(LogicTableType.GLOBAL, schemaName, tableName, columns, indexInfos, createTableSQL);
        return new GlobalTable(logicTable, backendTableInfos, tableConfigEntry);
    }

    public static TableHandler createNormalTable(String schemaName,
                                                 String tableName,
                                                 Partition partition,
                                                 List<SimpleColumnInfo> columns,
                                                 Map<String, IndexInfo> indexInfos,
                                                 String createTableSQL,
                                                 NormalTableConfig tableConfigEntry) {
        LogicTable logicTable = new LogicTable(LogicTableType.NORMAL, schemaName, tableName, columns, indexInfos, createTableSQL);
        return new NormalTable(logicTable, partition, tableConfigEntry);
    }

    public static ShardingTable createShardingTable(String schemaName,
                                                    String tableName,
                                                    List<Partition> backendTableInfos,
                                                    List<SimpleColumnInfo> columns,
                                                    CustomRuleFunction function,
                                                    Map<String, IndexInfo> indexInfos,
                                                    String createTableSQL,
                                                    List<ShardingIndexTable> shardingIndexTables,
                                                    ShardingTableConfig tableConfigEntry) {
        LogicTable logicTable = new LogicTable(LogicTableType.SHARDING, schemaName, tableName, columns, indexInfos, createTableSQL);
        return new ShardingTable(logicTable, backendTableInfos, function, shardingIndexTables, tableConfigEntry);
    }

    public SimpleColumnInfo getColumnByName(String name) {
        SimpleColumnInfo simpleColumnInfo = this.map.get(name);
        if (simpleColumnInfo == null) {
            SimpleColumnInfo simpleColumnInfo1 = this.map.get(name.toLowerCase());
            if (simpleColumnInfo1 == null) {
                return this.map.get(name.toUpperCase());
            } else {
                return simpleColumnInfo1;
            }
        } else {
            return simpleColumnInfo;
        }
    }

    public int getIndexBColumnName(String name) {
        SimpleColumnInfo columnByName = getColumnByName(name);
        return this.rawColumns.indexOf(columnByName);
    }

    public String getUniqueName() {
        return uniqueName;
    }

    public static String rewriteCreateTableSql(String sql, String schemaName, String tableName) {
        SQLStatement createTableAst = SQLUtils.parseSingleMysqlStatement(sql);
        if (createTableAst instanceof SQLCreateTableStatement) {
            SQLCreateTableStatement tableStatement = (SQLCreateTableStatement) createTableAst;
            tableStatement.setTableName("`" + tableName + "`");
            tableStatement.setSchema("`" + schemaName + "`");
        }
        if (createTableAst instanceof MySqlCreateTableStatement) {
            MySqlCreateTableStatement tableStatement = (MySqlCreateTableStatement) createTableAst;
            tableStatement.setTableName("`" + tableName + "`");
            tableStatement.setSchema("`" + schemaName + "`");
        }
        if (createTableAst instanceof SQLCreateViewStatement) {
            SQLExprTableSource tableSource = ((SQLCreateViewStatement) createTableAst).getTableSource();
            MycatSQLExprTableSourceUtil.setSqlExprTableSource(schemaName, tableSource.getTableName(), tableSource);
        }
        return createTableAst.toString();
    }

    public List<String> getFieldNames() {
        return this.rawColumns.stream().map(i -> i.getColumnName()).collect(Collectors.toList());
    }

    public List<String> getShardingKeys() {
        return this.rawColumns.stream().filter(i -> i.isShardingKey()).map(i -> i.getColumnName()).collect(Collectors.toList());
    }
}
