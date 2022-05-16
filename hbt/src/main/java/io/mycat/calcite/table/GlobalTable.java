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

import com.google.common.collect.ImmutableList;
import io.mycat.*;
import io.mycat.config.GlobalTableConfig;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.plug.sequence.SequenceGenerator;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static io.mycat.util.CreateTableUtils.createPhysicalTable;

public class GlobalTable implements GlobalTableHandler {
    private final LogicTable logicTable;
    private final List<Partition> backendTableInfos;
    private GlobalTableConfig tableConfig;


    public GlobalTable(LogicTable logicTable,
                       List<Partition> backendTableInfos,
                       GlobalTableConfig tableConfigEntry) {
        this.logicTable = logicTable;
        this.backendTableInfos = backendTableInfos;
        this.tableConfig = tableConfigEntry;
    }

//    @Override
//    public Function<MySqlInsertStatement, Iterable<ParameterizedValues>> insertHandler() {
//        return sqlStatement1 -> {
//            SQLExprTableSource tableSource = sqlStatement1.getTableSource();
//            return updateHandler(tableSource, sqlStatement1);
//        };
//    }

//    @NotNull
//    private Iterable<ParameterizedValues> updateHandler(SQLExprTableSource tableSource,  SQLStatement sqlStatement1) {
//        return ()->new Iterator<ParameterizedValues>() {
//            Iterator<DataNode> iterator = backendTableInfos.iterator();
//            @Override
//            public boolean hasNext() {
//                return iterator.hasNext();
//            }
//
//            @Override
//            public ParameterizedValues next() {
//                DataNode next = iterator.next();
//                tableSource.setExpr(next.getTargetSchemaTable());
//                return TextUpdateInfo.create(next.getTargetName(), Collections.singletonList(sqlStatement1.toString()));
//            }
//        };
//    }

//    @Override
//    public Function<MySqlUpdateStatement, Iterable<TextUpdateInfo>> updateHandler() {
//        return sqlStatement1 -> {
//            SQLExprTableSource tableSource = (SQLExprTableSource)sqlStatement1.getTableSource();
//            return updateHandler(tableSource, sqlStatement1);
//        };
//    }

//    @Override
//    public Function<MySqlDeleteStatement, Iterable<TextUpdateInfo>> deleteHandler() {
//        return sqlStatement1 -> {
//            SQLExprTableSource tableSource = (SQLExprTableSource)sqlStatement1.getTableSource();
//            return updateHandler(tableSource, sqlStatement1);
//        };
//    }

    @Override
    public LogicTableType getType() {
        return LogicTableType.GLOBAL;
    }

    @Override
    public String getSchemaName() {
        return logicTable.getSchemaName();
    }

    @Override
    public String getTableName() {
        return logicTable.getTableName();
    }

    @Override
    public String getCreateTableSQL() {
        return logicTable.getCreateTableSQL();
    }

    @Override
    public List<SimpleColumnInfo> getColumns() {
        return logicTable.getRawColumns();
    }

    @Override
    public Map<String, IndexInfo> getIndexes() {
        return logicTable.getIndexes();
    }

    @Override
    public SimpleColumnInfo getColumnByName(String name) {
        return logicTable.getColumnByName(name);
    }

    @Override
    public SimpleColumnInfo getAutoIncrementColumn() {
        return logicTable.getAutoIncrementColumn();
    }

    @Override
    public String getUniqueName() {
        return logicTable.getUniqueName();
    }
    @Override
    public Supplier<Number> nextSequence() {
        SequenceGenerator sequenceGenerator = MetaClusterCurrent.wrapper(SequenceGenerator.class);
        return sequenceGenerator.getSequence(getUniqueName());
    }

    @Override
    public void createPhysicalTables() {
        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        List<Partition> partitions = (List) ImmutableList.builder()
                .addAll(getGlobalDataNode()).build();
        partitions.stream().parallel().forEach(node -> createPhysicalTable(jdbcConnectionManager, node, getCreateTableSQL()));
    }


    @Override
    public void dropPhysicalTables() {
        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        for (Partition partition : getGlobalDataNode()) {
            try (DefaultConnection connection = jdbcConnectionManager.getConnection(partition.getTargetName())) {
                connection.deleteTable(getSchemaName(), getTableName());
            }
        }
    }

    public Partition getDataNode() {
        return getGlobalDataNode().get(0);
    }

    @Override
    public List<Partition> getGlobalDataNode() {
        return backendTableInfos;
    }

    public GlobalTableConfig getTableConfig() {
        return tableConfig;
    }
}