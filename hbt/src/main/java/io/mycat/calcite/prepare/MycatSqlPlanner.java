/**
 * Copyright (C) <2019>  <chen junwen>
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
package io.mycat.calcite.prepare;

import io.mycat.PlanRunner;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.calcite.CalciteRunners;
import io.mycat.calcite.MycatCalciteDataContext;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.table.EnumerableTable;
import io.mycat.calcite.table.MycatTransientSQLTableScan;
import io.mycat.calcite.table.SingeTargetSQLTable;
import io.mycat.upondb.MycatDBContext;
import io.mycat.upondb.ProxyInfo;
import io.mycat.util.Explains;
import lombok.SneakyThrows;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author Junwen Chen
 **/
public class MycatSqlPlanner implements PlanRunner, Proxyable {
    private final RelNode relNode;
    private final MycatSQLPrepareObject prepare;
    private final MycatCalciteDataContext mycatCalciteDataContext;
    private final String sql;


    @SneakyThrows
    public MycatSqlPlanner(MycatSQLPrepareObject prepare, String sql,SqlNode sqlNode, MycatDBContext uponDBContext) {
        this.sql = sql;
        this.prepare = prepare;
        this.mycatCalciteDataContext = MycatCalciteSupport.INSTANCE.create(uponDBContext);
        MycatCalcitePlanner planner = MycatCalciteSupport.INSTANCE.createPlanner(mycatCalciteDataContext);
        this.relNode = Objects.requireNonNull(CalciteRunners.compile(planner, sql,sqlNode, prepare.isForUpdate()));
    }

    public List<String> explain() {
        RelDataType rowType = relNode.getRowType();
        return Explains.explain(prepare.getSql(),
                null,
                MycatCalciteSupport.INSTANCE.dumpMetaData(rowType),
                MycatCalciteSupport.INSTANCE.convertToHBTText(relNode, mycatCalciteDataContext),
                MycatCalciteSupport.INSTANCE.convertToMycatRelNodeText(this.relNode, mycatCalciteDataContext));
    }


    @Override
    public RowBaseIterator run() {
        return CalciteRunners.run(sql,this.mycatCalciteDataContext, relNode);
    }

    public ProxyInfo tryGetProxyInfo() {
        if (relNode == null) {
            return null;
        }
        if (relNode instanceof MycatTransientSQLTableScan) {
            MycatTransientSQLTableScan sqlTableScan = (MycatTransientSQLTableScan)relNode;
            SingeTargetSQLTable singeTargetSQLTable = sqlTableScan.getTable().unwrap(SingeTargetSQLTable.class);
            return new ProxyInfo(singeTargetSQLTable.getTargetName(), singeTargetSQLTable.getSql(), prepare.isForUpdate());
        }
        return null;
    }

    static class ProxyAssertion extends RelShuttleImpl {
        final List<SingeTargetSQLTable> list = new ArrayList<>();
        boolean hasSetOp = false;

        @Override
        public RelNode visit(TableScan scan) {
            SingeTargetSQLTable unwrap = scan.getTable().unwrap(SingeTargetSQLTable.class);
            if (unwrap != null) {
                list.add(unwrap);
            }
            return super.visit(scan);
        }

        @Override
        public RelNode visit(LogicalUnion union) {
            hasSetOp = true;
            return super.visit(union);
        }


        @Override
        public RelNode visit(LogicalIntersect intersect) {
            hasSetOp = true;
            return super.visit(intersect);
        }

        @Override
        public RelNode visit(LogicalMinus minus) {
            hasSetOp = true;
            return super.visit(minus);
        }

        public boolean isAllowProxy() {
            return list.size() == 1 && !hasSetOp;
        }
    }

}