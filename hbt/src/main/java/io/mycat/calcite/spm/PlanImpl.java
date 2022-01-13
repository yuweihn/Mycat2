/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.calcite.spm;

import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.google.common.collect.ImmutableMultimap;
import io.mycat.AsyncMycatDataContextImpl;
import io.mycat.DrdsSqlWithParams;
import io.mycat.ExplainDetail;
import io.mycat.MycatDataContext;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.calcite.CodeExecuterContext;
import io.mycat.calcite.ExplainWriter;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.MycatRel;
import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.physical.MycatInsertRel;
import io.mycat.calcite.physical.MycatSQLTableLookup;
import io.mycat.calcite.physical.MycatUpdateRel;
import io.mycat.calcite.plan.ColocatedPlanner;
import io.mycat.calcite.resultset.CalciteRowMetaData;
import io.mycat.calcite.table.MycatTransientSQLTableScan;
import io.mycat.vertx.VertxExecuter;
import io.mycat.vertx.VertxUpdateExecuter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.util.Util;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.stream.Collectors;

public class PlanImpl implements Plan {
    private final MycatRel relNode;
    private final Type type;
    private final CodeExecuterContext executerContext;
    public final List<String> aliasList;
    public static PlanImpl of(MycatRel relNode,
                              CodeExecuterContext executerContext,
                              List<String> aliasList) {
        return new PlanImpl(relNode, executerContext, aliasList);
    }

    public PlanImpl(MycatRel relNode,
                    CodeExecuterContext executerContext,
                    List<String> aliasList) {
        this.relNode = relNode;
        this.aliasList = aliasList;
        this.type = Type.PHYSICAL;
        this.executerContext = executerContext;
    }

    public PlanImpl(MycatInsertRel relNode) {
        this.type = Type.INSERT;
        this.relNode = relNode;
        this.executerContext = null;
        this.aliasList = Collections.emptyList();
    }

    public PlanImpl(MycatUpdateRel relNode) {
        this.type = Type.UPDATE;
        this.relNode = relNode;
        this.executerContext = null;
        this.aliasList = Collections.emptyList();
    }

    @Override
    public Type getType() {
        return type;
    }

    @Override
    public CodeExecuterContext getCodeExecuterContext() {
        return executerContext;
    }

    public MycatUpdateRel getUpdatePhysical() {
        return (MycatUpdateRel) (relNode);
    }

    public MycatInsertRel getInsertPhysical() {
        return (MycatInsertRel) (relNode);
    }

    @Override
    public MycatRel getMycatRel() {
        return (MycatRel) relNode;
    }

    public List<String> explain(MycatDataContext dataContext, DrdsSqlWithParams drdsSql, boolean code) {
        ArrayList<String> list = new ArrayList<>();
        ExplainWriter explainWriter = new ExplainWriter();

        switch (this.type) {
            case PHYSICAL:

                Optional<ExplainDetail> singleViewOptional = ColocatedPlanner.executeQuery(dataContext, this, drdsSql);
                if (singleViewOptional.isPresent()){
                    ExplainDetail explainDetail = singleViewOptional.get();
                    list.add("ColocatedPushDown:");
                    list.add(explainDetail.toString());
                }
                list.add("\n");
                list.add("Plan:");
                String s = dumpPlan();
                list.addAll(Arrays.asList(s.split("\n")));
                List<SpecificSql> map = specificSql(drdsSql,dataContext);
                for (SpecificSql specificSql : map) {
                    list.addAll(Arrays.asList(specificSql.toString().split("\n")));
                }
                if (code) {
                    list.add("Code:");
                    list.addAll(Arrays.asList(getCodeExecuterContext().getCodeContext().getCode().split("\n")));
                }
                break;
            case UPDATE: {
                Collection<VertxExecuter.EachSQL> eachSQLS = VertxUpdateExecuter.explainUpdate(drdsSql, dataContext);
                for (VertxExecuter.EachSQL eachSQL : eachSQLS) {
                    list.add(eachSQL.toString());
                }
                break;
            }
            case INSERT: {
                Iterable<VertxExecuter.EachSQL> eachSQLS = (VertxExecuter.explainInsert((SQLInsertStatement) drdsSql.getParameterizedStatement(), drdsSql.getParams()));
                for (VertxExecuter.EachSQL eachSQL : eachSQLS) {
                    list.add(eachSQL.toString());
                }
                break;
            }
            default:
                throw new IllegalStateException("Unexpected value: " + this.type);
        }
        for (String s1 : explainWriter.getText().split("\n")) {
            list.add(s1);
        }
        return list.stream().filter(i -> !i.isEmpty()).collect(Collectors.toList());
    }

    @NotNull
    public String dumpPlan() {
        return MycatCalciteSupport.INSTANCE.convertToMycatRelNodeText(getMycatRel()).replaceAll("\r", "");
    }

    @NotNull
    @Override
    public List<SpecificSql> specificSql(DrdsSqlWithParams drdsSql) {
        return specificSql(drdsSql,5);
    }

    public List<SpecificSql> specificSql(DrdsSqlWithParams drdsSql,MycatDataContext mycatDataContext){
        return specificSql(drdsSql,mycatDataContext.getMergeUnionSize());
    }
    @NotNull
    public List<SpecificSql> specificSql(DrdsSqlWithParams drdsSql,int mergeUnionSize) {
        List<SpecificSql> res = new ArrayList<>();
        getMycatRel().accept(new RelShuttleImpl() {
            @Override
            protected RelNode visitChildren(RelNode relNode) {
                List< Each> sqls  = new ArrayList<>();
                String parameterizedSql = "";
                if (relNode instanceof MycatView) {
                    MycatView view =  (MycatView)relNode;
                    SqlNode sqlTemplate = view.getSQLTemplate(false);
                    ImmutableMultimap<String, SqlString> apply = view.apply(mergeUnionSize,sqlTemplate, AsyncMycatDataContextImpl.getSqlMap(executerContext.getConstantMap(),view, drdsSql,drdsSql.getHintDataNodeFilter()), drdsSql.getParams());
                    ImmutableMultimap<String, SqlString> stringImmutableMultimap =apply;
                    for (Map.Entry<String, SqlString> entry : (stringImmutableMultimap.entries())) {
                        SqlString sqlString = new SqlString(
                                entry.getValue().getDialect(),
                                (Util.toLinux(entry.getValue().getSql())),
                                entry.getValue().getDynamicParameters());
                        sqls.add(new Each(entry.getKey(), sqlString.getSql()));
                    }
                    if (relNode instanceof MycatView){
                        parameterizedSql = ((MycatView) relNode).getSql();
                    }
                    if (relNode instanceof MycatTransientSQLTableScan){
                        parameterizedSql = ((MycatTransientSQLTableScan) relNode).getSql();
                    }
                    res.add(new SpecificSql(relNode.getDigest(),parameterizedSql,sqls));
                }else if (relNode instanceof MycatSQLTableLookup){
                    MycatView right = ((MycatSQLTableLookup) relNode).getRight();
                    right.accept(this);
                }

                return super.visitChildren(relNode);
            }
        });
        return res;
    }

    @Override
    public MycatRowMetaData getMetaData() {
        MycatRel mycatRel = (MycatRel) relNode;
        List<RelDataTypeField> fieldList = mycatRel.getRowType().getFieldList();
        return new CalciteRowMetaData(fieldList,aliasList);
    }
}