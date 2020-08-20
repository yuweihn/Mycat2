package io.mycat.hbt4.logical.rules;

import io.mycat.hbt4.MycatConvention;
import io.mycat.hbt4.MycatConverterRule;
import io.mycat.hbt4.MycatRules;
import io.mycat.hbt4.logical.rel.MycatCorrelate;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

public class MycatCorrelateRule extends MycatConverterRule {
    /**
     * Creates a MycatSortRule.
     */
    public MycatCorrelateRule(MycatConvention out,
                         RelBuilderFactory relBuilderFactory) {
        super(Correlate.class, (Predicate<RelNode>) r -> true, MycatRules.convention, out,
                relBuilderFactory, "MycatCorrelateRule");
    }

    @Override
    public RelNode convert(RelNode rel) {
        Correlate correlate = (Correlate) rel;
        RelOptCluster cluster = rel.getCluster();
        return  MycatCorrelate.create(
                rel.getTraitSet().replace(MycatConvention.INSTANCE),
                convert(correlate.getLeft(),MycatConvention.INSTANCE),
                convert(correlate.getRight(),MycatConvention.INSTANCE),
                correlate.getCorrelationId(),
                correlate.getRequiredColumns(),
                correlate.getJoinType()
                );
    }
}