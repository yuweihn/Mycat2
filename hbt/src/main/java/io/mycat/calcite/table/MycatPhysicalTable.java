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
package io.mycat.calcite.table;

import io.mycat.BackendTableInfo;
import io.mycat.QueryBackendTask;
import io.mycat.calcite.CalciteUtls;
import io.mycat.calcite.MycatCalciteDataContext;
import io.mycat.calcite.resultset.MyCatResultSetEnumerable;
import io.mycat.metadata.TableHandler;
import lombok.Getter;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.TransientTable;
import org.apache.calcite.schema.TranslatableTable;

import java.util.List;

/**
 * @author Junwen Chen
 **/
@Getter
public class MycatPhysicalTable extends MycatTableBase implements TransientTable, ProjectableFilterableTable, TranslatableTable {
    final MycatLogicTable logicTable;
    final BackendTableInfo backendTableInfo;//真实表名

    public MycatPhysicalTable(MycatLogicTable logicTable, BackendTableInfo backendTableInfo) {
        this.logicTable = logicTable;
        this.backendTableInfo = backendTableInfo;
    }

    @Override
    public TableHandler logicTable() {
        return logicTable.logicTable();
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, int[] projects) {
        String backendTaskSQL = CalciteUtls.getBackendTaskSQL(filters,
                logicTable().getColumns(),
                CalciteUtls.getColumnList(logicTable(),projects), backendTableInfo);

        MycatCalciteDataContext root1 = (MycatCalciteDataContext) root;
        MyCatResultSetEnumerable.GetRow getRow = (mycatRowMetaData, targetName, sql) -> {
            return root1.getUponDBContext().query(mycatRowMetaData, targetName, sql);
        };
        return new MyCatResultSetEnumerable(getRow,root1.getCancelFlag(),getRowType(), new QueryBackendTask(backendTableInfo.getTargetName(),backendTaskSQL));
    }

    public String getTargetName() {
        return backendTableInfo.getTargetName();
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        return LogicalTableScan.create(context.getCluster(),relOptTable);
    }
}
