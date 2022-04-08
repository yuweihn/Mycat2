package io.mycat.calcite.rewriter;

import io.mycat.HintTools;
import io.mycat.calcite.localrel.LocalAggregate;
import io.mycat.calcite.logical.MycatView;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.mycat.calcite.localrel.LocalRules.normalize;

public class MycatAggDistinctRule extends RelRule<MycatAggDistinctRule.Config> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MycatAggDistinctRule.class);
    /**
     * Creates a RelRule.
     *
     * @param config
     */
    public MycatAggDistinctRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        try {
            Aggregate topAggregate = call.rel(0);
            MycatView mycatView = call.rel(2);
            RelHint lastAggHint = HintTools.getLastPushAggHint(topAggregate.getHints());
            if (lastAggHint != null) {
                if ("push_down_agg_distinct".equalsIgnoreCase(lastAggHint.hintName)) {

                    if (topAggregate.getAggCallList().size() == 1 && topAggregate.getGroupSet().isEmpty()) {
                        List<AggregateCall> aggCallList = topAggregate.getAggCallList();
                        if (aggCallList.size() == 1) {
                            AggregateCall aggregateCall = aggCallList.get(0);
                            if (aggregateCall.getAggregation().kind == SqlKind.COUNT) {
                                Aggregate distinctAgg = call.rel(1);
                                if (distinctAgg.getAggCallList().isEmpty() && !distinctAgg.getGroupSet().isEmpty()) {
                                    opt(call, topAggregate, mycatView);
                                    return;
                                }
                            }
                        }
                    }
                    Aggregate aggregate = topAggregate;
                    MycatView input = mycatView;
                    SQLRBORewriter.aggregate(input, LocalAggregate.create(aggregate, input)).ifPresent(new Consumer<RelNode>() {
                        @Override
                        public void accept(RelNode res) {
                            call.transformTo(normalize(res));
                        }
                    });
                    return;
                }
            }
        }catch (Throwable throwable){
            LOGGER.debug("MycatAggDistinctRule occurs exception",throwable);
        }
    }

    private void opt(RelOptRuleCall call, Aggregate topAggregate, MycatView mycatView) {
        RelBuilder builder = call.builder();
        builder.push(mycatView.getRelNode().getInput(0));
        List<AggregateCall> aggCallList = topAggregate.getAggCallList();
        List<RexInputRef> collect = aggCallList.get(0).getArgList().stream().map(i -> builder.field(i)).collect(Collectors.toList());
        RelBuilder.AggCall count = builder.count(true, "mycatCountDistinct", collect);
        builder.aggregate(builder.groupKey(), count);
        MycatView newView = mycatView.changeTo(builder.build());
        builder.push(newView);
        builder.aggregate(builder.groupKey(), builder.sum(builder.field(0)));
        call.transformTo(RelOptUtil.createCastRel(builder.build(), topAggregate.getRowType(), true));
    }

    public interface Config extends RelRule.Config {
        MycatAggDistinctRule.Config DEFAULT = EMPTY.as(MycatAggDistinctRule.Config.class)
                .withOperandFor();

        @Override
        default MycatAggDistinctRule toRule() {
            return new MycatAggDistinctRule(this);
        }

        default MycatAggDistinctRule.Config withOperandFor() {
            return withOperandSupplier(b0 ->
                    b0.operand(Aggregate.class)
                            .oneInput(i -> i.operand(Aggregate.class).predicate(rel -> rel.getAggCallList().isEmpty() && !rel.getGroupSet().isEmpty())
                                    .oneInput(j -> j.operand(MycatView.class).predicate(rel -> rel.allowPushdown()).noInputs())))
                    .withDescription("MycatAggDistinctRule")
                    .as(MycatAggDistinctRule.Config.class);
        }
    }
}
