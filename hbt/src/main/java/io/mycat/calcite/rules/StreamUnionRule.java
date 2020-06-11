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
package io.mycat.calcite.rules;

import com.google.common.collect.ImmutableList;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.table.MycatSQLTableScan;
import io.mycat.calcite.table.StreamUnionTable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Junwen Chen
 **/
/*

 */
public class StreamUnionRule extends RelOptRule {
    public static final StreamUnionRule INSTANCE  = new StreamUnionRule();

    public StreamUnionRule() {
        super(operandJ(Union.class, null, input -> input.getInputs().size() >= 2, any()), "UnionRule");
    }


    /**
     * @param call todo result set with order，backend
     */
    @Override
    public void onMatch(RelOptRuleCall call) {

        Union union = (Union) call.rels[0];
        List<RelNode> inputs = union.getInputs();
        int i = 0;
        for (RelNode relNode : inputs) {
            HepRelVertex relNode1 = (HepRelVertex) relNode;
            relNode = relNode1.getCurrentRel();
            RelOptTable table = relNode.getTable();
            if (table!=null){
                MycatSQLTableScan unwrap = table.unwrap(MycatSQLTableScan.class);
               if (unwrap!=null){
                   if(!unwrap.existsEnumerable()){
                       i++;
                   }
               }
            }
        }

        if (i == inputs.size()){
            List<MycatSQLTableScan> collect = new ArrayList<>(inputs.size());
            for (RelNode relNode : inputs) {
                HepRelVertex relNode1 = (HepRelVertex) relNode;
                relNode = relNode1.getCurrentRel();
                RelOptTable table = relNode.getTable();
                if (table!=null){
                    collect.add(table.unwrap(MycatSQLTableScan.class));
                }
            }
            StreamUnionTable streamUnionTable = new StreamUnionTable(collect);
            RelOptTable relOptTable = RelOptTableImpl.create(
                   null,
                    streamUnionTable.getRowType(MycatCalciteSupport.INSTANCE.TypeFactory),
                    streamUnionTable,
                    ImmutableList.of(collect.toString()));
            call.transformTo(LogicalTableScan.create(union.getCluster(),relOptTable,ImmutableList.of()));
            return;
        }

        if (inputs.size() > 2) {
            RelBuilder builder = call.builder();
            RelNode relNode = inputs.stream().reduce((relNode1, relNode2) -> {
                builder.clear();
                builder.push(relNode1);
                builder.push(relNode2);
                return builder.union(!union.isDistinct()).build();
            }).get();
            call.transformTo(relNode);
        }
    }
}