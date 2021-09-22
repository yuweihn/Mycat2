/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mycat.calcite;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.mycat.calcite.physical.*;
import io.mycat.calcite.rules.*;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.trace.CalciteTrace;
import org.slf4j.Logger;

import java.util.List;

/**
 * Rules and relational operators for
 * {@link MycatConvention}
 * calling convention.
 * <p>
 * 1.注意点 转换时候注意目标的表达式是否能接受源表达式,比如有不支持的自定义函数,排序项,分组项
 */
public class MycatRules {
    public final static Convention IN_CONVENTION = Convention.NONE;

    private MycatRules() {

    }

    protected static final Logger LOGGER = CalciteTrace.getPlannerTracer();

    public static final RelFactories.ProjectFactory PROJECT_FACTORY =
            (input, hints, projects, fieldNames) -> {
                final RelOptCluster cluster = input.getCluster();
                final RelDataType rowType =
                        RexUtil.createStructType(cluster.getTypeFactory(), projects,
                                fieldNames, SqlValidatorUtil.F_SUGGESTER);
                return new MycatProject(cluster, input.getTraitSet(), input, projects,
                        rowType);
            };

    public static final RelFactories.FilterFactory FILTER_FACTORY =
            (input, condition, variablesSet) -> {
                Preconditions.checkArgument(variablesSet.isEmpty(),
                        "MycatFilter does not allow variables");
                return MycatFilter.create(
                        input.getTraitSet(), input, condition);
            };

    public static final RelFactories.JoinFactory JOIN_FACTORY =
            (left, right, hints, condition, variablesSet, joinType, semiJoinDone) -> {
                final RelOptCluster cluster = left.getCluster();
                final RelTraitSet traitSet = cluster.traitSetOf(left.getConvention());
                return MycatNestedLoopJoin.create(traitSet,hints, left, right, condition, joinType);
            };

    static final RelFactories.CorrelateFactory CORRELATE_FACTORY =
            (left, right, correlationId, requiredColumns, joinType) -> {
                throw new UnsupportedOperationException("MycatCorrelate");
            };

    public static final RelFactories.SortFactory SORT_FACTORY =
            (input, collation, offset, fetch) -> {
                throw new UnsupportedOperationException("MycatSort");
            };

    public static final RelFactories.ExchangeFactory EXCHANGE_FACTORY =
            (input, distribution) -> {
                throw new UnsupportedOperationException("MycatExchange");
            };

    public static final RelFactories.SortExchangeFactory SORT_EXCHANGE_FACTORY =
            (input, distribution, collation) -> {
                throw new UnsupportedOperationException("MycatSortExchange");
            };

    public static final RelFactories.AggregateFactory AGGREGATE_FACTORY =
            (input, hints, groupSet, groupSets, aggCalls) -> {
                final RelOptCluster cluster = input.getCluster();
                final RelTraitSet traitSet = cluster.traitSetOf(input.getConvention());
                return MycatHashAggregate.create(traitSet,hints, input, groupSet,
                        groupSets, aggCalls);
            };

    public static final RelFactories.MatchFactory MATCH_FACTORY =
            (input, pattern, rowType, strictStart, strictEnd, patternDefinitions,
             measures, after, subsets, allRows, partitionKeys, orderKeys,
             interval) -> {
                throw new UnsupportedOperationException("MycatMatch");
            };

    public static final RelFactories.SetOpFactory SET_OP_FACTORY =
            (kind, inputs, all) -> {
                RelNode input = inputs.get(0);
                RelOptCluster cluster = input.getCluster();
                final RelTraitSet traitSet = cluster.traitSetOf(input.getConvention());
                switch (kind) {
                    case UNION:
                        return MycatUnion.create(traitSet, inputs, all);
                    case INTERSECT:
                        return MycatIntersect.create(traitSet, inputs, all);
                    case EXCEPT:
                        return MycatMinus.create(traitSet, inputs, all);
                    default:
                        throw new AssertionError("unknown: " + kind);
                }
            };

    public static final RelFactories.ValuesFactory VALUES_FACTORY =
            (cluster, rowType, tuples) -> {
                throw new UnsupportedOperationException();
            };

    public static final RelFactories.TableScanFactory TABLE_SCAN_FACTORY =
            (toRelContext, table) -> {
                throw new UnsupportedOperationException();
            };

    public static final RelFactories.SnapshotFactory SNAPSHOT_FACTORY =
            (input, period) -> {
                throw new UnsupportedOperationException();
            };

    /**
     * A {@link RelBuilderFactory} that creates a {@link RelBuilder} that will
     * create Mycat relational expressions for everything.
     */
    public static final RelBuilderFactory MYCAT_BUILDER =
            RelBuilder.proto(
                    Contexts.of(PROJECT_FACTORY,
                            FILTER_FACTORY,
                            JOIN_FACTORY,
                            SORT_FACTORY,
                            EXCHANGE_FACTORY,
                            SORT_EXCHANGE_FACTORY,
                            AGGREGATE_FACTORY,
                            MATCH_FACTORY,
                            SET_OP_FACTORY,
                            VALUES_FACTORY,
                            TABLE_SCAN_FACTORY,
                            SNAPSHOT_FACTORY));

    public static List<RelOptRule> rules() {
        ImmutableList.Builder<RelOptRule> builder = ImmutableList.builder();
        return builder.add(
//                AggregateReduceFunctionsRule.Config.DEFAULT.toRule(),
                CoreRules.AGGREGATE_REDUCE_FUNCTIONS,
                MycatJoinRule.INSTANCE,
                MycatCalcRule.INSTANCE,
                MycatProjectRule.INSTANCE,
                MycatFilterRule.INSTANCE,
                MycatAggregateRule.INSTANCE,
                MycatMemSortRule.INSTANCE,
                MycatUnionRule.INSTANCE,
                MycatIntersectRule.INSTANCE,
                MycatMinusRule.INSTANCE,
                MycatTableModificationRule.INSTANCE,
                MycatValuesRule.INSTANCE,
                MycatSortAggRule.INSTANCE,
                MycatCorrelateRule.INSTANCE,
                MycatTopNRule.INSTANCE,
                MycatRepeatUnionRule.INSTANCE,
                MycatTableSpoolRule.INSTANCE,
                MycatWinodwRule.INSTANCE,
//                , MycatBatchNestedLoopJoinRule.INSTANCE

//                CoreRules.FILTER_TO_CALC,
//                CoreRules.PROJECT_TO_CALC,
                CoreRules.CALC_REMOVE,
//                CoreRules.CALC_MERGE,
                CoreRules.CALC_TO_WINDOW,
                MycatMergeJoinRule.INSTANCE
//                MycatBottomTableScanViewRule.Config.DEFAULT.toRule()
        )
                .addAll(MycatExtraSortRule.RULES)
//                .addAll(MycatSingleViewRule.RULES)
//                .addAll(MycatBiRelViewRule.RULES)
//                .add(MycatBottomTableScanViewRule.Config.DEFAULT.toRule())
                .build();
    }

    /**
     * Returns whether this Mycat data source can implement a given aggregate
     * function.
     */
    private static boolean canImplement(SqlAggFunction aggregation, SqlDialect sqlDialect) {
        return sqlDialect.supportsAggregateFunction(aggregation.getKind());
    }


}
