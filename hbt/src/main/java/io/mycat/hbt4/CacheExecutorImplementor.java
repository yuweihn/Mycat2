package io.mycat.hbt4;

import io.mycat.MycatDataContext;
import io.mycat.api.collector.RowBaseIteratorCacher;
import io.mycat.calcite.resultset.CalciteRowMetaData;
import io.mycat.calcite.resultset.EnumeratorRowIterator;
import io.mycat.hbt4.executor.MycatInsertExecutor;
import io.mycat.hbt4.executor.MycatUpdateExecutor;
import io.mycat.hbt4.executor.TempResultSetFactory;
import io.mycat.hbt4.executor.TempResultSetFactoryImpl;
import io.mycat.hbt4.logical.rel.MycatInsertRel;
import io.mycat.hbt4.logical.rel.MycatUpdateRel;
import io.mycat.util.Response;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;

import java.util.Objects;

public class CacheExecutorImplementor  extends ExecutorImplementorImpl {
    private final Object key;

    public static CacheExecutorImplementor create(Object key,MycatDataContext context, Response response){
        TempResultSetFactory tempResultSetFactory = new TempResultSetFactoryImpl();
        DatasourceFactory datasourceFactory = new DefaultDatasourceFactory(context);
        return new CacheExecutorImplementor(key,datasourceFactory,tempResultSetFactory);
    }

    public CacheExecutorImplementor(Object key,  DatasourceFactory factory, TempResultSetFactory tempResultSetFactory) {
        super(factory, tempResultSetFactory);
        this.key = Objects.requireNonNull(key);
    }

    @Override
    public void implementRoot(MycatRel rel) {
        if (rel instanceof MycatInsertRel){
            return;
        }
        if (rel instanceof MycatUpdateRel){
            return;
        }
        Executor executor = rel.implement(this);
        RelDataType rowType = rel.getRowType();
        EnumeratorRowIterator rowIterator = new EnumeratorRowIterator(new CalciteRowMetaData(rowType.getFieldList()),
                Linq4j.asEnumerable(() -> executor.outputObjectIterator()).enumerator(), () -> {
        });
        RowBaseIteratorCacher.put(this.key,rowIterator);
    }
}