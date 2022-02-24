package io.mycat;

import cn.mycat.vertx.xa.MySQLManager;
import cn.mycat.vertx.xa.XaSqlConnection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import hu.akarnokd.rxjava3.operators.Flowables;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.calcite.*;
import io.mycat.calcite.executor.MycatPreparedStatementUtil;
import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.rewriter.Distribution;
import io.mycat.calcite.rewriter.ValueIndexCondition;
import io.mycat.calcite.rewriter.ValuePredicateAnalyzer;
import io.mycat.calcite.spm.ParamHolder;
import io.mycat.calcite.table.GlobalTable;
import io.mycat.calcite.table.MycatTransientSQLTableScan;
import io.mycat.calcite.table.NormalTable;
import io.mycat.calcite.table.ShardingTable;
import io.mycat.newquery.NewMycatConnection;
import io.mycat.prototypeserver.mysql.VisualTableHandler;
import io.mycat.querycondition.QueryType;
import io.mycat.router.CustomRuleFunction;
import io.mycat.util.VertxUtil;
import io.mycat.vertx.VertxExecuter;
import io.reactivex.rxjava3.core.BackpressureStrategy;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.impl.future.PromiseInternal;
import lombok.Getter;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.SqlString;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

@Getter
public abstract class AsyncMycatDataContextImpl extends NewMycatDataContextImpl {
    protected final static Logger LOGGER = LoggerFactory.getLogger(AsyncMycatDataContextImpl.class);
    protected final static Logger FULL_TABLE_SCAN_LOGGER = LoggerFactory.getLogger("FULL_TABLE_SCAN_LOGGER");
    public static int FULL_TABLE_SCAN_LIMIT = 1024;
    final Map<String, Future<NewMycatConnection>> transactionConnnectionMap = new HashMap<>();// int transaction
    final List<Future<NewMycatConnection>> connnectionFutureCollection = new LinkedList<>();//not int transaction
    final Map<String, List<Observable<Object[]>>> shareObservable = new HashMap<>();


    public AsyncMycatDataContextImpl(MycatDataContext dataContext,
                                     CodeExecuterContext context,
                                     DrdsSqlWithParams drdsSqlWithParams) {
        super(dataContext, context, drdsSqlWithParams);
    }

    public synchronized Future<NewMycatConnection> getConnection(String key) {
        XaSqlConnection transactionSession = (XaSqlConnection) context.getTransactionSession();
        if (context.isInTransaction()) {
            return transactionConnnectionMap
                    .computeIfAbsent(key, s -> transactionSession.getConnection(key));
        }
        MySQLManager mySQLManager = MetaClusterCurrent.wrapper(MySQLManager.class);
        Future<NewMycatConnection> connection = mySQLManager.getConnection(key);
        connnectionFutureCollection.add(connection);
        return connection;
    }

    public synchronized void recycleConnection(String key, Future<NewMycatConnection> connectionFuture) {
        XaSqlConnection transactionSession = (XaSqlConnection) context.getTransactionSession();
        if (context.isInTransaction()) {
            transactionConnnectionMap.put(key, connectionFuture);
            return;
        }
        connectionFuture = connectionFuture.flatMap(c -> c.close().mapEmpty());
        transactionSession.addCloseFuture(connectionFuture.mapEmpty());
        connnectionFutureCollection.add(Objects.requireNonNull(connectionFuture));
    }

    public static interface Queryer<T> {

        Observable<T> runQuery(Future<NewMycatConnection> sessionConnection, String sql, List<Object> extractParams, MycatRowMetaData calciteRowMetaData);
    }

    @NotNull
    public synchronized <T> List<Observable<T>> getObservables(ImmutableMultimap<String, SqlString> expand, MycatRowMetaData calciteRowMetaData, Queryer<T> queryer) {
        LinkedList<Observable<T>> observables = new LinkedList<>();
        for (Map.Entry<String, SqlString> entry : expand.entries()) {
            String key = context.resolveDatasourceTargetName(entry.getKey());
            SqlString sqlString = entry.getValue();
            Observable<T> observable = Observable.create(emitter -> {
                Future<NewMycatConnection> sessionConnection = getConnection(key);
                PromiseInternal<NewMycatConnection> promise = VertxUtil.newPromise();
                Observable<T> innerObservable = Objects.requireNonNull(queryer.runQuery(sessionConnection,
                        sqlString.getSql(),
                        MycatPreparedStatementUtil.extractParams(drdsSqlWithParams.getParams(), sqlString.getDynamicParameters()), calciteRowMetaData));
                innerObservable.subscribe(objects -> {
                            emitter.onNext((objects));
                        },
                        throwable -> {
                            sessionConnection.onSuccess(c -> {
                                        //close connection?
                                        emitter.onError(throwable);
                                        promise.tryFail(throwable);
                                    })
                                    .onFailure(t -> {
                                        emitter.onError(throwable);
                                        promise.tryFail(t);
                                    });
                        }, () -> {
                            sessionConnection.onSuccess(c -> {
                                promise.tryComplete(c);
                            }).onFailure(t -> {
                                emitter.onError(t);
                                promise.tryFail(t);
                            });
                            ;
                        });
                recycleConnection(key,
                        promise.future()
                                .onSuccess(c -> {
                                    emitter.onComplete();
                                })
                                .onFailure(t -> {
                                    emitter.onError(t);
                                }));
            });
            observables.add(observable);
        }
        return observables;
    }

    public synchronized CompositeFuture endFuture() {
        return CompositeFuture.join((List) ImmutableList.builder()
                .addAll(transactionConnnectionMap.values())
                .addAll(connnectionFutureCollection).build());
    }

    public abstract List<Observable<Object[]>> getObservableList(String node);

    public static final class HbtMycatDataContextImpl extends AsyncMycatDataContextImpl {

        public HbtMycatDataContextImpl(MycatDataContext dataContext, CodeExecuterContext context) {
            super(dataContext, context, DrdsRunnerHelper.preParse("select 1", null));
        }

        @Override
        public List<Observable<Object[]>> getObservableList(String node) {
            MycatRelDatasourceSourceInfo mycatRelDatasourceSourceInfo = codeExecuterContext.getRelContext().get(node);
            MycatTransientSQLTableScan relNode = (MycatTransientSQLTableScan) mycatRelDatasourceSourceInfo.getRelNode();
            ImmutableMultimap<String, SqlString> multimap = ImmutableMultimap.of(relNode.getTargetName(), new SqlString(MycatSqlDialect.DEFAULT, relNode.getSql()));
            return getObservables(multimap, mycatRelDatasourceSourceInfo.getColumnInfo(),
                    (sessionConnection, sql, extractParams, calciteRowMetaData) -> VertxExecuter.runQuery(sessionConnection, sql, extractParams, calciteRowMetaData)
            );
        }

        @Override
        public Observable<Object[]> getObservable(String node, Function1 function1, Comparator comparator, int offset, int fetch) {
            return null;
        }
    }


    @Getter
    public static final class SqlMycatDataContextImpl extends AsyncMycatDataContextImpl {

        private DrdsSqlWithParams drdsSqlWithParams;
        private ConcurrentMap<String, List<PartitionGroup>> cache = new ConcurrentHashMap<>();
        MycatDataContext dataContext;

        public SqlMycatDataContextImpl(MycatDataContext dataContext, CodeExecuterContext context, DrdsSqlWithParams drdsSqlWithParams) {
            super(dataContext, context, drdsSqlWithParams);
            this.drdsSqlWithParams = drdsSqlWithParams;
            this.dataContext = dataContext;
        }

        public List<Observable<Object[]>> getObservableList(String node) {
            if (shareObservable.containsKey(node)) {
                return (shareObservable.get(node));
            }
            MycatRelDatasourceSourceInfo mycatRelDatasourceSourceInfo = this.codeExecuterContext.getRelContext().get(node);
            MycatView view = mycatRelDatasourceSourceInfo.getRelNode();
            List<PartitionGroup> sqlMap = getPartition(node).get();
            if ((sqlMap.size() > FULL_TABLE_SCAN_LIMIT) && FULL_TABLE_SCAN_LOGGER.isInfoEnabled()) {
                FULL_TABLE_SCAN_LOGGER.info(" warning sql:{},partition count:{},limit:{},it may be a full table scan.",
                        drdsSqlWithParams.toString(),sqlMap.size(),FULL_TABLE_SCAN_LIMIT);
            }
            boolean share = mycatRelDatasourceSourceInfo.refCount > 0;
            List<Observable<Object[]>> observables = getObservables((view
                            .apply(dataContext.getMergeUnionSize(), mycatRelDatasourceSourceInfo.getSqlTemplate(), sqlMap, drdsSqlWithParams.getParams())), mycatRelDatasourceSourceInfo.getColumnInfo(),
                    (sessionConnection, sql, extractParams, calciteRowMetaData) -> VertxExecuter.runQuery(sessionConnection, sql, extractParams, calciteRowMetaData)
            );
            if (share) {
                observables = observables.stream().map(i -> i.share()).collect(Collectors.toList());
                shareObservable.put(node, observables);
            }
            return observables;
        }

        public Optional<List<PartitionGroup>> getPartition(String node) {
            MycatRelDatasourceSourceInfo mycatRelDatasourceSourceInfo = this.codeExecuterContext.getRelContext().get(node);
            if (mycatRelDatasourceSourceInfo == null) return Optional.empty();
            MycatView view = mycatRelDatasourceSourceInfo.getRelNode();
            return Optional.ofNullable(cache.computeIfAbsent(node, s -> getSqlMap(codeExecuterContext.getConstantMap(), view, drdsSqlWithParams, drdsSqlWithParams.getHintDataNodeFilter())));
        }


        @Override
        public Observable<Object[]> getObservable(String node, Function1 function1, Comparator comparator, int offset, int fetch) {
            List<Observable<Object[]>> observableList = getObservableList(node).stream().map(i -> i.subscribeOn(Schedulers.computation())).collect(Collectors.toList());

            Iterable<Flowable<Object[]>> collect = observableList.stream().map(s -> Flowable.fromObservable(s, BackpressureStrategy.BUFFER)).collect(Collectors.toList());
            Flowable<Object[]> flowable = Flowables.orderedMerge(collect, (o1, o2) -> {
                Object left = function1.apply(o1);
                Object right = function1.apply(o2);
                return comparator.compare(left, right);
            });
            if (offset > 0) {
                flowable = flowable.skip(offset);
            }
            if (fetch > 0 && fetch != Integer.MAX_VALUE) {
                flowable = flowable.take(fetch);
            }
            return flowable.toObservable();
        }

    }


    @Override
    public Observable<Object[]> getObservable(String node) {
        return Observable.concatEager(getObservableList(node).stream().map(i -> i.subscribeOn(Schedulers.computation())).collect(Collectors.toList()));

    }

    public static List<PartitionGroup> getSqlMap(Map<RexNode, RexNode> constantMap,
                                                 MycatView view,
                                                 DrdsSqlWithParams drdsSqlWithParams,
                                                 Optional<List<PartitionGroup>> hintDataMapping) {
        Distribution distribution = view.getDistribution();

        Distribution.Type type = distribution.type();
        switch (type) {
            case BROADCAST: {
                Map<String, Partition> builder = new HashMap<>();
                String targetName = null;
                for (GlobalTable globalTable : distribution.getGlobalTables()) {
                    if (targetName == null) {
                        int i = ThreadLocalRandom.current().nextInt(0, globalTable.getGlobalDataNode().size());
                        Partition partition = globalTable.getGlobalDataNode().get(i);
                        targetName = partition.getTargetName();
                    }
                    builder.put(globalTable.getUniqueName(), globalTable.getDataNode());
                }
                return Collections.singletonList(new PartitionGroup(targetName, builder));
            }
            case PHY:
                Map<String, Partition> builder = new HashMap<>();
                String targetName = null;
                for (GlobalTable globalTable : distribution.getGlobalTables()) {
                    builder.put(globalTable.getUniqueName(), globalTable.getDataNode());
                }
                for (NormalTable normalTable : distribution.getNormalTables()) {
                    if (targetName == null) {
                        targetName = normalTable.getDataNode().getTargetName();
                    }
                    builder.put(normalTable.getUniqueName(), normalTable.getDataNode());
                }
                return Collections.singletonList(new PartitionGroup(targetName, builder));
            case SHARDING:
                if (hintDataMapping.isPresent()) {
                    return hintDataMapping.get();
                }

                ShardingTable shardingTable = distribution.getShardingTables().get(0);
                RexBuilder rexBuilder = MycatCalciteSupport.RexBuilder;
                RexNode condition = view.getCondition().orElse(MycatCalciteSupport.RexBuilder.makeLiteral(true));
                List<RexNode> inputConditions = new ArrayList<>(constantMap.size() + 1);

                inputConditions.add(condition);
                for (Map.Entry<RexNode, RexNode> rexNodeRexNodeEntry : constantMap.entrySet()) {
                    inputConditions.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, rexNodeRexNodeEntry.getKey(), rexNodeRexNodeEntry.getValue()));
                }
                ParamHolder paramHolder = ParamHolder.CURRENT_THREAD_LOCAL.get();
                paramHolder.setData(drdsSqlWithParams.getParams(), drdsSqlWithParams.getTypeNames());
                try {
                    ArrayList<RexNode> res = new ArrayList<>(inputConditions.size());
                    MycatRexExecutor.INSTANCE.reduce(rexBuilder, inputConditions, res);
                    condition = res.get(0);
                    ValuePredicateAnalyzer predicateAnalyzer = new ValuePredicateAnalyzer(shardingTable.keyMetas(true), shardingTable.getColumns().stream().map(i -> i.getColumnName()).collect(Collectors.toList()));
                    Map<QueryType, List<ValueIndexCondition>> indexConditionMap = predicateAnalyzer.translateMatch(condition);
                    List<Partition> partitions = ValueIndexCondition.getPartitions(shardingTable.getShardingFuntion(), indexConditionMap, drdsSqlWithParams.getParams());
                    return mapSharding(view, partitions);
                } finally {
                    paramHolder.clear();
                }
            default:
                throw new IllegalStateException("Unexpected value: " + distribution.type());
        }
    }

    public static List<PartitionGroup> mapSharding(MycatView view, List<Partition> partitionList) {
        Distribution distribution = view.getDistribution();
        List<ShardingTable> shardingTableList = distribution.getShardingTables();
        ShardingTable primaryShardingTable = shardingTableList.get(0);
        CustomRuleFunction primaryShardingFunction = primaryShardingTable.getShardingFuntion();
        HashMap<String, Partition> groupTemplate = new HashMap<>();
        for (NormalTable normalTable : distribution.getNormalTables()) {//可能存在错误的数据分布,但是错误的数据分布访问不到
            groupTemplate.put(normalTable.getUniqueName(), normalTable.getDataNode());
        }
        for (GlobalTable globalTable : distribution.getGlobalTables()) {
            groupTemplate.put(globalTable.getUniqueName(), globalTable.getDataNode());
        }
        if (distribution.getShardingTables().size() == 1) {
            List<PartitionGroup> res = new ArrayList<>(partitionList.size());
            for (Partition partition : partitionList) {
                HashMap<String, Partition> map = new HashMap<>(groupTemplate);
                map.put(primaryShardingTable.getUniqueName(), partition);
                res.add(new PartitionGroup(partition.getTargetName(), map));
            }
            return res;
        } else {
            List<ShardingTable> joinShardingTables = shardingTableList.subList(1, shardingTableList.size());
            List<PartitionGroup> res = new ArrayList<>(partitionList.size());
            for (Partition primaryPartition : partitionList) {
                HashMap<String, Partition> map = new HashMap<>(groupTemplate);
                map.put(primaryShardingTable.getUniqueName(), primaryPartition);
                for (ShardingTable joinShardingTable : joinShardingTables) {
                    CustomRuleFunction joinFunction = joinShardingTable.function();
                    if (primaryShardingFunction.isSameDistribution(joinFunction)) {
                        Partition joinPartition = joinFunction.getPartition(primaryShardingFunction.indexOf(primaryPartition));
                        map.put(joinShardingTable.getUniqueName(), joinPartition);
                    } else if (primaryShardingFunction.isSameTargetFunctionDistribution(joinFunction)) {
                        List<Partition> joinPartitions = joinShardingTable.getPartitionsByTargetName(primaryPartition.getTargetName());
                        if (joinPartitions.size() != 1) {
                            throw new IllegalArgumentException("wrong partition " + joinPartitions + " in " + view);
                        }
                        map.put(joinShardingTable.getUniqueName(), joinPartitions.get(0));
                    }
                }
                res.add(new PartitionGroup(primaryPartition.getTargetName(), map));
            }
            return res;
        }
    }


    @Override
    public Observable<Object[]> getTableObservable(String schemaName, String tableName) {
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        TableHandler tableHandler = metadataManager.getTable(schemaName, tableName);
        VisualTableHandler visualTableHandler = (VisualTableHandler) tableHandler;
        return visualTableHandler.scanAll();
    }
}
