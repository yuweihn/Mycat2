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
package io.mycat.vertx;

import cn.mycat.vertx.xa.XaSqlConnection;
import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLReplaceable;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.alibaba.druid.sql.visitor.MycatSQLEvalVisitorUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.mycat.PreparedStatement;
import io.mycat.Process;
import io.mycat.*;
import io.mycat.api.collector.MySQLColumnDef;
import io.mycat.api.collector.MysqlPayloadObject;
import io.mycat.api.collector.MysqlObjectArrayRow;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.calcite.CodeExecuterContext;
import io.mycat.calcite.DrdsRunnerHelper;
import io.mycat.calcite.ExecutorProviderImpl;
import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.spm.QueryPlanner;
import io.mycat.calcite.table.GlobalTable;
import io.mycat.calcite.table.NormalTable;
import io.mycat.calcite.table.ShardingTable;
import io.mycat.newquery.MysqlCollector;
import io.mycat.newquery.NewMycatConnection;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.mysqlclient.MySQLClient;
import io.vertx.sqlclient.*;
import lombok.*;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.ArrayBindable;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class VertxExecuter {
    private static final Logger LOGGER = LoggerFactory.getLogger(VertxExecuter.class);


    public static Future<long[]> runUpdate(Future<NewMycatConnection> sqlConnectionFuture, String sql, List<Object> params) {
        return sqlConnectionFuture.flatMap(c -> c.update(sql, params))
                .map(r -> new long[]{r.getAffectRows(), r.getLastInsertId()});
    }

    public static Future<long[]> simpleUpdate(MycatDataContext context, boolean insert, boolean xa, boolean onlyFirstSum, Collection<EachSQL> eachSQLs) {
        if (xa && (eachSQLs.size() > 1)) {
            return simpleUpdate(context, insert, xa, onlyFirstSum, (Iterable<EachSQL>) eachSQLs);
        }
        return simpleUpdate(context, insert, false, onlyFirstSum, (Iterable<EachSQL>) eachSQLs);
    }

    public static Future<long[]> simpleUpdate(MycatDataContext context, boolean insert, boolean xa, boolean onlyFirstSum, Iterable<EachSQL> eachSQLs) {
        Function<Void, Future<long[]>> function = new Function<Void, Future<long[]>>() {
            final long[] sum = new long[]{0, 0};

            @Override
            public Future<long[]> apply(Void unused) {
                XaSqlConnection transactionSession = (XaSqlConnection) context.getTransactionSession();
                ConcurrentHashMap<String, Future<NewMycatConnection>> map = new ConcurrentHashMap<>();
                final AtomicBoolean firstRequest = new AtomicBoolean(true);
                for (EachSQL eachSQL : eachSQLs) {

                    String target = context.resolveDatasourceTargetName(eachSQL.getTarget(), true);
                    String sql = eachSQL.getSql();
                    List<Object> params = eachSQL.getParams();

                    Future<NewMycatConnection> connectionFuture = map.computeIfAbsent(target, s -> transactionSession.getConnection(target));
                    Future<long[]> future;
                    if (insert) {
                        future = VertxExecuter.runInsert(connectionFuture, sql, params);
                    } else {
                        future = VertxExecuter.runUpdate(connectionFuture, sql, params);
                    }
                    Future<NewMycatConnection> returnConnectionFuture = future.map((Function<long[], Void>) longs2 -> {
                        if (!onlyFirstSum) {
                            synchronized (sum) {
                                sum[0] = sum[0] + longs2[0];
                                sum[1] = Math.max(sum[1], longs2[1]);
                            }
                        } else if (firstRequest.compareAndSet(true, false)) {
                            sum[0] = longs2[0];
                            sum[1] = longs2[1];
                        }
                        return null;
                    }).mapEmpty().flatMap(c -> Future.succeededFuture(connectionFuture.result()));
                    map.put(target, returnConnectionFuture);
                }
                List<Future<NewMycatConnection>> futures = new ArrayList<>(map.values());
                return CompositeFuture.join((List) futures).map(sum);
            }
        };
        if (xa) {
            return wrapAsXaTransaction(context, function);
        } else {
            return Future.succeededFuture().flatMap(o -> function.apply(null));
        }
    }

    @Getter
    @EqualsAndHashCode
    @ToString
    public static class EachSQL {
        String target;
        String sql;
        List<Object> params;

        public EachSQL(String target, String sql, List<Object> params) {
            this.target = target;
            this.sql = sql;
            this.params = params;
        }
    }

    @SneakyThrows
    public static Collection<EachSQL> explainUpdate(DrdsSqlWithParams drdsSqlWithParams, MycatDataContext context) {
        SQLUpdateStatement statement = drdsSqlWithParams.getParameterizedStatement();
        List<Object> params = drdsSqlWithParams.getParams();
        SQLExprTableSource tableSource = (SQLExprTableSource) statement.getTableSource();
        String alias = SQLUtils.normalize(tableSource.computeAlias());
        String tableName = SQLUtils.normalize(tableSource.getTableName());
        String schemaName = SQLUtils.normalize(tableSource.getSchema());

        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);

        TableHandler table = metadataManager.getTable(schemaName, tableName);

        switch (table.getType()) {
            case SHARDING: {
                ShardingTable shardingTable = (ShardingTable) table;
                SimpleColumnInfo primaryKey = shardingTable.getPrimaryKey();

                SQLExpr where = statement.getWhere();

                SQLSelectStatement sqlSelectStatement = new SQLSelectStatement();
                SQLSelectQueryBlock queryBlock = sqlSelectStatement.getSelect().getQueryBlock();
                queryBlock.addWhere(where);
                queryBlock.setFrom(tableSource.clone());

                Set<String> selectKeys = new HashSet<>();

                if (primaryKey != null) {
                    selectKeys.add(primaryKey.getColumnName());
                }

                ImmutableList<ShardingTable> shardingTables = (ImmutableList) ImmutableList.builder().add(shardingTable).addAll(shardingTable.getIndexTables()).build();


                Map<String, SQLExpr> columnMap = shardingTables.stream().flatMap(s -> s.getColumns().stream().filter(i -> i.isShardingKey())).map(i -> i.getColumnName())
                        .distinct().collect(Collectors.toMap(k -> k, v -> SQLUtils.toSQLExpr(v + " = ? ")));

                shardingTable.getColumns().stream().filter(i -> shardingTable.getShardingFuntion().isShardingKey(i.getColumnName())).collect(Collectors.toList());

                selectKeys.addAll(columnMap.keySet());

                for (String selectKey : selectKeys) {
                    queryBlock.addSelectItem(new SQLPropertyExpr(alias, selectKey));
                }

                DrdsSqlWithParams queryDrdsSqlWithParams = DrdsRunnerHelper.preParse(sqlSelectStatement, null);

                QueryPlanner planCache = MetaClusterCurrent.wrapper(QueryPlanner.class);
                List<CodeExecuterContext> acceptedMycatRelList = planCache.getAcceptedMycatRelList(queryDrdsSqlWithParams);
                CodeExecuterContext codeExecuterContext = acceptedMycatRelList.get(0);
                MycatView mycatRel = (MycatView) codeExecuterContext.getMycatRel();
                List<PartitionGroup> sqlMap = AsyncMycatDataContextImpl.getSqlMap(Collections.emptyMap(), mycatRel, queryDrdsSqlWithParams, drdsSqlWithParams.getHintDataNodeFilter());

                List<Partition> partitions = sqlMap.stream().map(partitionGroup -> partitionGroup.get(shardingTable.getUniqueName())).collect(Collectors.toList());
                List<EachSQL> res = new ArrayList<>(partitions.size());
                for (Partition partition : partitions) {
                    SQLUpdateStatement eachSql = statement.clone();
                    SQLExprTableSource eachTableSource = (SQLExprTableSource) eachSql.getTableSource();
                    eachTableSource.setExpr(partition.getTable());
                    eachTableSource.setSchema(partition.getSchema());
                    res.add(new EachSQL(partition.getTargetName(), eachSql.toString(), params));
                }
                if (shardingTable.getIndexTables().isEmpty()) {
                    return res;
                }
                AsyncMycatDataContextImpl.SqlMycatDataContextImpl sqlMycatDataContext =
                        new AsyncMycatDataContextImpl.SqlMycatDataContextImpl(context, codeExecuterContext, queryDrdsSqlWithParams);

                ArrayBindable bindable = ExecutorProviderImpl.INSTANCE.prepare(codeExecuterContext);

                Object bindObservable = bindable.bindObservable(sqlMycatDataContext);
                try {
                    List<Object[]> objects;
                    if (bindObservable instanceof Observable) {
                        objects = ((Observable<Object[]>) bindObservable).toList().blockingGet();
                    } else {
                        objects = ((Enumerable<Object[]>) (Enumerable) bindObservable).toList();
                    }

                    Object[][] list = Iterables.toArray(objects, Object[].class);
                    if (list.length > 1000) {
                        throw new IllegalArgumentException("The number of update rows exceeds the limit.");
                    }

                    for (ShardingTable indexTable : shardingTable.getIndexTables()) {
                        SQLUpdateStatement eachStatement = new SQLUpdateStatement();
                        eachStatement.setFrom(new SQLExprTableSource());
                        SQLExprTableSource sqlTableSource = (SQLExprTableSource) eachStatement.getTableSource();
                        sqlTableSource.setExpr(indexTable.getTableName());
                        sqlTableSource.setSchema(indexTable.getSchemaName());

                        SQLBinaryOpExprGroup sqlBinaryOpExprGroup = new SQLBinaryOpExprGroup(SQLBinaryOperator.Equality, DbType.mysql);


                        RelDataType rowType = codeExecuterContext.getMycatRel().getRowType();

                        ArrayList<Integer> exactKeys = new ArrayList<>();
                        for (SimpleColumnInfo column : indexTable.getColumns()) {
                            SQLExpr sqlExpr = columnMap.get(column.getColumnName());
                            sqlExpr = sqlExpr.clone();
                            sqlBinaryOpExprGroup.add(sqlExpr);
                            RelDataTypeField field = rowType.getField(column.getColumnName(), false, false);
                            exactKeys.add(field.getIndex());
                        }


                        eachStatement.setWhere(sqlBinaryOpExprGroup);

                        for (Object[] eachParams : list) {
                            List<Object> newEachParams = new ArrayList<>();
                            for (Integer exactKey : exactKeys) {
                                newEachParams.add(eachParams[exactKey]);
                            }

                            Collection<EachSQL> eachSQLS = explainUpdate(new DrdsSqlWithParams(eachStatement.toString(),
                                            newEachParams,
                                            false,
                                            Collections.emptyList(),
                                            Collections.emptyList(),
                                            Collections.emptyList()),
                                    context);

                            res.addAll(eachSQLS);
                        }
                    }
                    return res;
                } finally {
                    context.getTransactionSession().closeStatementState().toCompletionStage().toCompletableFuture().get(1, TimeUnit.SECONDS);
                }
            }
            case GLOBAL: {
                GlobalTable globalTable = (GlobalTable) table;
                List<EachSQL> res = new ArrayList<>(globalTable.getGlobalDataNode().size());
                for (Partition partition : globalTable.getGlobalDataNode()) {
                    SQLUpdateStatement eachSql = statement.clone();
                    SQLExprTableSource eachTableSource = (SQLExprTableSource) eachSql.getTableSource();
                    eachTableSource.setExpr(partition.getTable());
                    eachTableSource.setSchema(partition.getSchema());
                    res.add(new EachSQL(partition.getTargetName(), eachSql.toString(), params));
                }
                return res;
            }
            case NORMAL: {
                NormalTable normalTable = (NormalTable) table;
                List<EachSQL> res = new ArrayList<>(1);
                Partition partition = normalTable.getDataNode();
                SQLUpdateStatement eachSql = statement.clone();
                SQLExprTableSource eachTableSource = (SQLExprTableSource) eachSql.getTableSource();
                eachTableSource.setExpr(partition.getTable());
                eachTableSource.setSchema(partition.getSchema());
                res.add(new EachSQL(partition.getTargetName(), eachSql.toString(), params));
                return res;
            }
            case CUSTOM:
                throw new UnsupportedOperationException();
            default:
                throw new IllegalStateException("Unexpected value: " + table.getType());
        }
    }

    public static List<EachSQL> rewriteInsertBatchedStatements(Iterable<EachSQL> eachSQLs) {
        return rewriteInsertBatchedStatements(eachSQLs, 1000);
    }

    public static List<EachSQL> rewriteInsertBatchedStatements(Iterable<EachSQL> eachSQLs, int batchSize) {
        @Data
        @AllArgsConstructor
        @EqualsAndHashCode
        class key {
            String target;
            String sql;
        }

        Map<key, SQLInsertStatement> map = new ConcurrentHashMap<>();
        final Function<Map.Entry<key, SQLInsertStatement>, EachSQL> finalFunction = i -> {
            key key = i.getKey();
            SQLInsertStatement value = i.getValue();
            return new EachSQL(key.getTarget(), value.toString(), Collections.emptyList());
        };
        LinkedList<EachSQL> res = new LinkedList<>();
        for (EachSQL eachSQL : eachSQLs) {

            String target = eachSQL.getTarget();
            String sql = eachSQL.getSql();
            SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);
            List<Object> sqlParams = eachSQL.getParams();

            if (sqlStatement instanceof SQLInsertStatement) {
                SQLInsertStatement insertStatement = (SQLInsertStatement) sqlStatement;


                key key = new key(target, sql);

                final SQLInsertStatement nowInsertStatement = map.computeIfAbsent(key, key1 -> {
                    SQLInsertStatement clone = insertStatement.clone();
                    clone.getValuesList().clear();
                    return clone;
                });
                MySqlASTVisitorAdapter mySqlASTVisitorAdapter = new MySqlASTVisitorAdapter() {
                    @Override
                    public boolean visit(SQLVariantRefExpr x) {
                        SQLReplaceable parent = (SQLReplaceable) x.getParent();
                        parent.replace(x, PreparedStatement.fromJavaObject(sqlParams.get(x.getIndex())));
                        return false;
                    }
                };


                List<EachSQL> list = new LinkedList<>();
                for (SQLInsertStatement.ValuesClause valuesClause : insertStatement.getValuesList()) {
                    valuesClause = valuesClause.clone();
                    nowInsertStatement.addValueCause(valuesClause);
                    valuesClause.accept(mySqlASTVisitorAdapter);

                    if (nowInsertStatement.getValuesList().size() >= batchSize) {
                        EachSQL e = finalFunction.apply(new AbstractMap.SimpleEntry(key, nowInsertStatement.clone()));
                        list.add(e);
                        nowInsertStatement.getValuesList().clear();
                    } else {
                        continue;
                    }
                }
                res.addAll(list);
            } else {
                res.add(eachSQL);
            }
        }
        res.addAll(map.entrySet().stream().map(finalFunction).collect(Collectors.toList()));
        return res;
    }

    @SneakyThrows
    public static List<EachSQL> explainInsert(SQLInsertStatement statementArg, List<Object> paramArg) {
        final SQLInsertStatement statement = statementArg.clone();

        SQLInsertStatement template = statement.clone();
        template.getColumns().clear();
        template.getValuesList().clear();

        SQLExprTableSource tableSource = statement.getTableSource();

        String tableName = SQLUtils.normalize(tableSource.getTableName());
        String schemaName = SQLUtils.normalize(tableSource.getSchema());

        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        ShardingTable table = (ShardingTable) metadataManager.getTable(schemaName, tableName);
        SimpleColumnInfo autoIncrementColumn = table.getAutoIncrementColumn();


        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        List<SQLName> columns = (List) statement.getColumns();
        if (columns.isEmpty()) {
            if (statement.getValues().getValues().size() == table.getColumns().size()) {
                for (SimpleColumnInfo column : table.getColumns()) {
                    statement.addColumn(new SQLIdentifierExpr("`" + column.getColumnName() + "`"));
                }
            }
        }
        boolean fillAutoIncrement = needFillAutoIncrement(table, columns);
        if (fillAutoIncrement) {
            columns.add(new SQLIdentifierExpr(autoIncrementColumn.getColumnName()));
        }
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        Map<String, Integer> columnMap = new HashMap<>();

        int index = 0;
        for (SQLName column : columns) {
            columnMap.put(SQLUtils.normalize(column.getSimpleName()), index);
            ++index;
        }

        List<List> paramsList = (!paramArg.isEmpty() && paramArg.get(0) instanceof List) ? (List) paramArg : Collections.singletonList(paramArg);

        return paramsList.stream().flatMap(params -> {
            List<EachSQL> sqls = new LinkedList<>();
            for (SQLInsertStatement.ValuesClause valuesClause : statement.getValuesList()) {

                valuesClause = valuesClause.clone();
                SQLInsertStatement primaryStatement = template.clone();
                primaryStatement.getColumns().addAll(columns);
                primaryStatement.getValuesList().add(valuesClause);
                List<SQLExpr> values = primaryStatement.getValues().getValues();

                if (fillAutoIncrement) {
                    Supplier<Number> stringSupplier = table.nextSequence();
                    values.add(PreparedStatement.fromJavaObject(stringSupplier.get()));
                }

                Map<String, List<RangeVariable>> variables = compute(columns, values, params);
                Partition mPartition = table.getShardingFuntion().calculateOne((Map) variables);

                SQLExprTableSource exprTableSource = primaryStatement.getTableSource();
                exprTableSource.setSimpleName(mPartition.getTable());
                exprTableSource.setSchema(mPartition.getSchema());


                sqls.add(new EachSQL(mPartition.getTargetName(), primaryStatement.toString(), getNewParams(params, primaryStatement)));


                for (ShardingTable indexTable : table.getIndexTables()) {


                    //  fillIndexTableShardingKeys(variables, indexTable);

                    Partition sPartition = indexTable.getShardingFuntion().calculateOne((Map) variables);

                    SQLInsertStatement eachStatement = template.clone();
                    eachStatement.getColumns().clear();

                    fillIndexTableShardingKeys(columnMap, values, indexTable.getColumns(), eachStatement);

                    SQLExprTableSource eachTableSource = eachStatement.getTableSource();
                    eachTableSource.setSimpleName(sPartition.getTable());
                    eachTableSource.setSchema(sPartition.getSchema());

                    sqls.add(new EachSQL(sPartition.getTargetName(), eachStatement.toString(), getNewParams(params, eachStatement)));
                }
            }
            return sqls.stream();
        }).collect(Collectors.toList());
    }

    private static void fillIndexTableShardingKeys(Map<String, Integer> columnMap, List<SQLExpr> values, List<SimpleColumnInfo> otherColumns, SQLInsertStatement eachStatement) {
        eachStatement.getColumns().addAll(otherColumns.stream().map((Function<SimpleColumnInfo, SQLName>) i -> new SQLIdentifierExpr(i.getColumnName())).collect(Collectors.toList()));
        eachStatement.addValueCause(new SQLInsertStatement.ValuesClause(
                otherColumns.stream().map(i -> {
                    Integer integer = columnMap.get(i.getColumnName());
                    if (integer != null) {
                        return values.get(integer);
                    }
                    return new SQLNullExpr();
                }).collect(Collectors.toList())
        ));
    }

    private static void fillIndexTableShardingKeys(Map<String, List<RangeVariable>> variables, ShardingTable indexTable) {
        for (String s : indexTable.getColumns().stream()
                .filter(i -> i.isShardingKey())
                .filter(i -> i.isNullable())
                .map(i -> i.getColumnName())
                .collect(Collectors.toList())) {
            variables.putIfAbsent(s, Collections.singletonList(new RangeVariable(s, RangeVariableType.EQUAL, null)));
        }
    }

    @NotNull
    private static List<Object> getNewParams(List params, SQLInsertStatement primaryStatement) {
        List<Object> newParams = new ArrayList<>();
        primaryStatement.accept(new MySqlASTVisitorAdapter() {
            @Override
            public boolean visit(SQLVariantRefExpr x) {
                newParams.add(params.get(x.getIndex()));
                return false;
            }
        });
        return newParams;
    }

    private static boolean needFillAutoIncrement(ShardingTable table, List<SQLName> columns) {
        SimpleColumnInfo autoIncrementColumn = table.getAutoIncrementColumn();
        if (autoIncrementColumn != null) {
            for (SQLName column : columns) {
                String columnName = SQLUtils.normalize(column.getSimpleName());
                if (SQLUtils.nameEquals(columnName, autoIncrementColumn.getColumnName())) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    public static Map<String, RangeVariable> compute(List<SQLName> columns,
                                                     List<SQLExpr> values,
                                                     List<Object> params) {
        Map<String, RangeVariable> variables = new HashMap<>(1);
        for (int i = 0; i < columns.size(); i++) {
            SQLExpr sqlExpr = values.get(i);
            Object o = null;
            if (sqlExpr instanceof SQLVariantRefExpr) {
                int index = ((SQLVariantRefExpr) sqlExpr).getIndex();
                o = params.get(index);
            } else if (sqlExpr instanceof SQLNullExpr) {
                o = null;
            } else {
                try {
                    o = MycatSQLEvalVisitorUtils.eval(DbType.mysql, sqlExpr, params);
                } catch (Throwable throwable) {
                    boolean success = false;
                    if (sqlExpr instanceof SQLMethodInvokeExpr) {
                        if (!((SQLMethodInvokeExpr) sqlExpr).getArguments().isEmpty()) {
                            SQLExpr sqlExpr1 = ((SQLMethodInvokeExpr) sqlExpr).getArguments().get(0);
                            if (sqlExpr1 instanceof SQLVariantRefExpr) {
                                int index = ((SQLVariantRefExpr) sqlExpr1).getIndex();
                                o = params.get(index);
                                success = true;
                            } else if (sqlExpr1 instanceof SQLCharExpr) { // 兼容 compress 等函数，放的是字符串
                                o = ((SQLCharExpr) sqlExpr1).getText();
                                success = true;
                            }
                        }
                    }
                    if (!success) {
                        throw throwable;
                    }
                }
            }
            String columnName = SQLUtils.normalize(columns.get(i).getSimpleName());
            variables.put(columnName, new RangeVariable(columnName, RangeVariableType.EQUAL, o));
        }
        return variables;
    }

//
//    public static Future<long[]> runMycatUpdateRel(MycatDataContext context, MycatUpdateRel updateRel, List<Object> params) {
//
//
//        MycatRouteUpdateCore mycatRouteUpdateCore = updateRel.getMycatRouteUpdateCore();
//        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
//        TableHandler table = metadataManager.getTable(mycatRouteUpdateCore.getSchemaName(), mycatRouteUpdateCore.getTableName());
//        List<Partition> partitions = Collections.emptyList();
//        switch (table.getType()) {
//            case SHARDING: {
//                ShardingTable shardingTable = (ShardingTable) table;
//                RexNode conditions = mycatRouteUpdateCore.getConditions();
//                ParamHolder paramHolder = ParamHolder.CURRENT_THREAD_LOCAL.get();
//                paramHolder.setData(params, null);
//                try {
//                    ArrayList<RexNode> res = new ArrayList<>(1);
//                    MycatRexExecutor.INSTANCE.reduce(MycatCalciteSupport.RexBuilder, Collections.singletonList(conditions), res);
//                    RexNode rexNode = res.get(0);
//                    ValuePredicateAnalyzer predicateAnalyzer = new ValuePredicateAnalyzer(shardingTable.keyMetas(), shardingTable.getColumns().stream().map(i -> i.getColumnName()).collect(Collectors.toList()));
//                    ValueIndexCondition indexCondition = predicateAnalyzer.translateMatch(rexNode);
//                    partitions = ValueIndexCondition.getObject(shardingTable.getShardingFuntion(), indexCondition, params);
//                } finally {
//                    paramHolder.clear();
//                }
//                break;
//            }
//            case GLOBAL: {
//                GlobalTable globalTable = (GlobalTable) table;
//                partitions = globalTable.getGlobalDataNode();
//                break;
//            }
//            case NORMAL: {
//                NormalTable normalTable = (NormalTable) table;
//                partitions = Collections.singletonList(normalTable.getDataNode());
//                break;
//            }
//            case CUSTOM:
//                throw new UnsupportedOperationException();
//        }
//        ArrayList<EachSQL> eachSQLS = new ArrayList<>();
//        SQLStatement sqlStatement = updateRel.getMycatRouteUpdateCore().getSqlStatement().clone();
//        for (Partition partition : partitions) {
//            sqlStatement.accept(new MySqlASTVisitorAdapter() {
//                @Override
//                public boolean visit(SQLExprTableSource x) {
//                    x.setSimpleName(partition.getTable());
//                    x.setSchema(partition.getSchema());
//                    return false;
//                }
//            });
//            String s = sqlStatement.toString();
//            eachSQLS.add(new EachSQL(partition.getTargetName(), s, params));
//        }
//        return simpleUpdate(context, true, true, eachSQLS);
//    }


    public static Future<long[]> wrapAsXaTransaction(MycatDataContext context, Function<Void, Future<long[]>> function) {
        TransactionSession sqlConnection = context.getTransactionSession();
        if ((!context.isInTransaction() && context.isAutocommit())) {
            Future<long[]> future = sqlConnection.begin().flatMap(function);
            return future.flatMap(longs -> sqlConnection.commit().map(longs))
                    .recover(throwable -> CompositeFuture.join(Future.failedFuture(throwable), sqlConnection.rollback()).mapEmpty());
        }
        return Future.succeededFuture().flatMap(o -> function.apply(null));
    }


    public static Observable<MysqlPayloadObject> runQueryOutputAsMysqlPayloadObject(Future<NewMycatConnection> connectionFuture,
                                                                                    String sql,
                                                                                    List<Object> values) {
        return Observable.create(emitter -> {
            // 连接到达
            connectionFuture.onSuccess(connection -> {
                // 预编译到达
                connection.prepareQuery(sql, values, new MysqlCollector() {

                    MycatRowMetaData mycatRowMetaData;

                    @Override
                    public void onColumnDef(MycatRowMetaData mycatRowMetaData) {
                        emitter.onNext(new MySQLColumnDef(this.mycatRowMetaData = mycatRowMetaData));
                    }

                    @Override
                    public void onRow(Object[] row) {
                        emitter.onNext(new MysqlObjectArrayRow(BaseRowObservable.getObjects(row, this.mycatRowMetaData)));
                    }

                    @Override
                    public void onComplete() {
                        emitter.onComplete();
                    }

                    @Override
                    public void onError(Exception e) {
                        emitter.onError(e);
                    }
                });
            });
            connectionFuture.onFailure(i -> emitter.onError(i));
        });
    }

    public static Observable<Object[]> runQuery(Future<NewMycatConnection> connectionFuture,
                                                String sql,
                                                List<Object> values,
                                                MycatRowMetaData rowMetaDataArg) {
        return Observable.create(emitter -> {
            // 连接到达
            connectionFuture.onSuccess(connection -> {
                // 预编译到达
                connection.prepareQuery(sql, values, new MysqlCollector() {

                    MycatRowMetaData mycatRowMetaData;

                    @Override
                    public void onColumnDef(MycatRowMetaData mycatRowMetaData) {
                        this.mycatRowMetaData = Optional.ofNullable(rowMetaDataArg).orElse(mycatRowMetaData);
                    }

                    @Override
                    public void onRow(Object[] row) {
                        emitter.onNext(BaseRowObservable.getObjects(row, this.mycatRowMetaData));
                    }

                    @Override
                    public void onComplete() {
                        emitter.onComplete();
                    }

                    @Override
                    public void onError(Exception e) {
                        emitter.onError(e);
                    }
                });
            });
            connectionFuture.onFailure(i -> emitter.onError(i));
        });
    }

    public static Future<long[]> runUpdate(Future<NewMycatConnection> sqlConnectionFuture, String sql) {
        return sqlConnectionFuture.flatMap(c -> c.update(sql)
                .map(r -> new long[]{r.getAffectRows(), r.getLastInsertId()}));
    }

    public static Future<long[]> runInsert(Future<NewMycatConnection> sqlConnectionFuture, String sql) {
        return sqlConnectionFuture.flatMap(c -> c.insert(sql)
                .map(r -> new long[]{r.getAffectRows(), r.getLastInsertId()}));
    }

    public static Future<long[]> runInsert(Future<NewMycatConnection> sqlConnectionFuture, String sql, List<Object> params) {
        return sqlConnectionFuture.flatMap(c -> c.insert(sql, params)
                .map(r -> new long[]{r.getAffectRows(), r.getLastInsertId()}));
    }

    public static Future<long[]> runUpdate(Map<String, List<Object>> updateMap,
                                           Future<NewMycatConnection> sqlConnectionFuture) {
        List<long[]> list = Collections.synchronizedList(new ArrayList<>());
        Future<Void> future = Future.succeededFuture();
        for (Map.Entry<String, List<Object>> e : updateMap.entrySet()) {
            String sql = e.getKey();
            List<Object> values = e.getValue();
            future = future.flatMap(new UpdateByConnection(sqlConnectionFuture, sql, values, list));
        }
        return future.map(sumUpdateResult(list));
    }

    public static Future<long[]> runInsert(
            Map<String, List<List<Object>>> insertMap,
            Future<SqlConnection> sqlConnectionFuture) {
        List<long[]> list = Collections.synchronizedList(new ArrayList<>());
        Future<Void> future = Future.succeededFuture();
        for (Map.Entry<String, List<List<Object>>> e : insertMap.entrySet()) {
            String sql = e.getKey();
            List<List<Object>> values = e.getValue();
            future = future.flatMap(unused -> {
                return sqlConnectionFuture
                        .flatMap(new Function<SqlConnection, Future<Void>>() {
                            @Override
                            public Future<Void> apply(SqlConnection connection) {
                                Future<Void> future2 = Process.getCurrentProcess().trace(connection).prepare(sql).flatMap(preparedStatement -> {
                                    List<Tuple> collect = values.stream().map(u -> Tuple.from(u)).collect(Collectors.toList());
                                    Future<RowSet<Row>> rowSetFuture = preparedStatement.query().executeBatch(collect);
                                    Future<Void> map = rowSetFuture.map(rows -> {
                                        int affectedRow = rows.rowCount();
                                        long lastInsertId = Optional.ofNullable(rows.property(MySQLClient.LAST_INSERTED_ID))
                                                .orElse(0L);
                                        list.add(new long[]{affectedRow, lastInsertId});
                                        return null;
                                    });
                                    return map;
                                });
                                return future2;
                            }
                        });
            });
        }
        return future.map(sumUpdateResult(list));
    }

    @NotNull
    private static Function<Void, long[]> sumUpdateResult(List<long[]> list) {
        return unused -> list.stream()
                .reduce(new long[]{0, 0},
                        (longs1, longs2) ->
                                new long[]{longs1[0] + longs2[0],
                                        Math.max(longs1[1], longs2[1])});
    }

    private static class SumUpdateResult implements Function<CompositeFuture, long[]> {
        private final boolean global;
        private final List<Future<long[]>> res;

        public SumUpdateResult(boolean global, List<Future<long[]>> res) {
            this.global = global;
            this.res = res;
        }

        @Override
        public long[] apply(CompositeFuture r) {
            return this.global ? res.get(0).result() :
                    res.stream().map(l -> l.result())
                            .reduce((longs, longs2) ->
                                    new long[]{longs[0] + longs2[0], Math.max(longs[1], longs2[1])})
                            .orElse(new long[2]);
        }
    }

    private static class UpdateByConnection implements Function {
        private final Future<NewMycatConnection> sqlConnectionFuture;
        private final String sql;
        private final List<Object> values;
        private final List<long[]> list;

        public UpdateByConnection(Future<NewMycatConnection> sqlConnectionFuture, String sql, List<Object> values, List<long[]> list) {
            this.sqlConnectionFuture = sqlConnectionFuture;
            this.sql = sql;
            this.values = values;
            this.list = list;
        }

        @Override
        public Object apply(Object unused) {
//            return sqlConnectionFuture
//                    .flatMap(connection -> Process.getCurrentProcess().trace(connection).prepare(sql)
//                            .flatMap(preparedStatement -> {
//                                Future<RowSet<Row>> rowSetFuture;
//                                if (!values.isEmpty() && values.get(0) instanceof List) {
//                                    rowSetFuture = preparedStatement.query().executeBatch(values.stream().map(i -> Tuple.from((List<Object>) i)).collect(Collectors.toList()));
//                                } else {
//                                    rowSetFuture = preparedStatement.query().execute(Tuple.from(values));
//                                }
//                                return rowSetFuture.map(new UpdateResultCollector());
//                            }));
            return null;
        }

        private class UpdateResultCollector implements Function<RowSet<Row>, Object> {
            @Override
            public Object apply(RowSet<Row> rows) {
                int affectedRow = rows.rowCount();
                long lastInsertId = Optional.ofNullable(rows.property(MySQLClient.LAST_INSERTED_ID))
                        .orElse(0L);
                list.add(new long[]{affectedRow, lastInsertId});
                return null;
            }
        }
    }
}
