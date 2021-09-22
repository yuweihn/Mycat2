//package io.mycat.calcite;
//
//import io.mycat.MycatDataContext;
//import io.mycat.calcite.logical.MycatView;
//import io.mycat.calcite.executor.*;
//import io.mycat.calcite.physical.MycatInsertRel;
//import io.mycat.calcite.physical.MycatUpdateRel;
//import io.mycat.util.Pair;
//import io.mycat.Response;
//import lombok.SneakyThrows;
//
//import java.util.List;
//
//public class ProxyExecutorImplementor extends ResponseExecutorImplementor  {
//
//    public static ProxyExecutorImplementor create(MycatDataContext context, Response response){
//        TempResultSetFactory tempResultSetFactory = new TempResultSetFactoryImpl();
//        DataSourceFactory datasourceFactory = new DefaultDatasourceFactory(context);
//        return new ProxyExecutorImplementor(context,datasourceFactory,tempResultSetFactory,response);
//    }
//
//    public ProxyExecutorImplementor(MycatDataContext context,DataSourceFactory factory,
//                                    TempResultSetFactory tempResultSetFactory,
//                                    Response response) {
//        super(context,factory, tempResultSetFactory, response);
//    }
//
//    @SneakyThrows
//    @Override
//    public void implementRoot(MycatRel rel, List<String> aliasList) {
//        try {
//            if (rel instanceof MycatInsertRel) {
//                Executor executor = super.implement((MycatInsertRel) rel);
//                MycatInsertExecutor insertExecutor = (MycatInsertExecutor) executor;
//                if (insertExecutor.isProxy()) {
//                    Pair<String, String> pair = insertExecutor.getSingleSql();
//                    response.proxyUpdate(pair.getKey(), pair.getValue());
//                } else {
//                    factory.open();
//                    runInsert(insertExecutor);
//                }
//                return;
//            }
//            if (rel instanceof MycatUpdateRel) {
//                Executor executor = super.implement((MycatUpdateRel) rel);
//                MycatUpdateExecutor updateExecutor = (MycatUpdateExecutor) executor;
//                if (updateExecutor.isProxy()) {
//                    Pair<String, String> pair = updateExecutor.getSingleSql();
//                    response.proxyUpdate(pair.getKey(), pair.getValue());
//                } else {
//                    factory.open();
//                    runUpdate(updateExecutor);
//                }
//                return;
//            }
//            if (rel instanceof MycatView) {
//                Executor executor = super.implement((MycatView) rel);
//                ViewExecutor viewExecutor = (ViewExecutor) executor;
//                if (viewExecutor.isProxy()) {
//                    Pair<String, String> singleSql = viewExecutor.getSingleSql();
//                    response.proxySelect(singleSql.getKey(), singleSql.getValue());
//                } else {
//                    factory.open();
//                    runQuery(rel, viewExecutor, aliasList);
//                }
//                return;
//            }
//            super.implementRoot(rel, aliasList);
//        }finally {
//            factory.close();
//        }
//    }
//}