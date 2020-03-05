package io.mycat.calcite.logic;

import io.mycat.QueryBackendTask;
import io.mycat.calcite.MyCatResultSetEnumerable;
import io.mycat.calcite.MycatCalciteContext;
import io.mycat.calcite.MycatCalciteDataContext;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.TransientTable;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.SqlDialect;


/**
 * chenjunwen
 */
public class MycatTransientSQLTable extends PreComputationSQLTable
        implements TransientTable, TranslatableTable {
    private final MycatConvention convention;
    private final RelNode input;

    public MycatTransientSQLTable(MycatConvention convention, RelNode input) {
        this.input = input;
        this.convention = convention;
    }

    public String getExplainSQL() {
        return getExplainSQL(input);
    }

    public String getExplainSQL(RelNode input) {
        SqlDialect dialect = convention.dialect;
        return MycatCalciteContext.INSTANCE.convertToSql(input, dialect);
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.copyType(input.getRowType());
    }

    public String getTargetName() {
        return convention.targetName;
    }

    public RelNode getRelNode() {
        return input;
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        return new MycatTransientSQLTableScan(context.getCluster(), convention, relOptTable, () -> getExplainSQL());
    }


    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        MycatCalciteDataContext root1 = (MycatCalciteDataContext) root;
        Enumerable<Object[]> preComputation = root1.removePreComputation(this);
        if (preComputation!=null){
            return preComputation;
        }
        String sql = getExplainSQL();
        return new MyCatResultSetEnumerable(root1, new QueryBackendTask(sql, convention.targetName));

    }
}