package io.mycat.hbt3;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;

public class MycatAggregateViewRule extends RelOptRule {
    public final static MycatAggregateViewRule INSTANCE = new MycatAggregateViewRule();

    public MycatAggregateViewRule() {
        super(operand(LogicalAggregate.class,
                operand(View.class, none())), "MycatFilterViewRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalAggregate up = call.rel(0);
        RelNode view = call.rel(1);
        if (RelMdSqlViews.aggregate(up)) {
            RelNode res = SQLRBORewriter.aggregate(view,up);
            if (res != null) {
                call.transformTo(res);
            }
        }
    }
}