package io.mycat.calcite.prepare;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.UpdateRowIterator;
import io.mycat.beans.mycat.EmptyMycatRowMetaData;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mycat.UpdateRowMetaData;
import io.mycat.upondb.UponDBContext;

import java.util.Arrays;
import java.util.List;

public abstract class QueryPlanRunnerImpl extends MycatSQLPrepareObject implements PlanRunner {

    public QueryPlanRunnerImpl(UponDBContext uponDBContext, String sql) {
        super(null,uponDBContext, sql);
    }

    public abstract void innerEun();

    @Override
    public RowBaseIterator run() {
        innerEun();
        return UpdateRowIterator.EMPTY;
    }

    @Override
    public PlanRunner plan(List<Object> params) {
        return this;
    }

    public abstract String innerExplain();

    @Override
    public List<String> explain() {
        return Arrays.asList(getClass().getSimpleName());
    }

    @Override
    public MycatRowMetaData prepareParams() {
        return EmptyMycatRowMetaData.INSTANCE;
    }

    @Override
    public MycatRowMetaData resultSetRowType() {
        return UpdateRowMetaData.INSTANCE;
    }

}