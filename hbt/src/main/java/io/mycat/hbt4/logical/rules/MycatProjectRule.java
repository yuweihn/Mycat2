/**
 * Copyright (C) <2020>  <chen junwen>
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
package io.mycat.hbt4.logical.rules;


import io.mycat.hbt4.MycatConvention;
import io.mycat.hbt4.MycatConverterRule;
import io.mycat.hbt4.MycatRules;
import io.mycat.hbt4.logical.rel.MycatProject;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * Rule to convert a {@link Project} to
 * an {@link MycatProjectRule}.
 */
public class MycatProjectRule extends MycatConverterRule {

    /**
     * Creates a MycatProjectRule.
     */
    public MycatProjectRule(final MycatConvention out,
                            RelBuilderFactory relBuilderFactory) {
        super(Project.class, project ->
                        true,
                MycatRules.convention, out, relBuilderFactory, "MycatProjectRule");
    }

    private static boolean userDefinedFunctionInProject(Project project) {
        CheckingUserDefinedFunctionVisitor visitor = new CheckingUserDefinedFunctionVisitor();
        for (RexNode node : project.getChildExps()) {
            node.accept(visitor);
            if (visitor.containsUserDefinedFunction()) {
                return true;
            }
        }
        return false;
    }

    public RelNode convert(RelNode rel) {
        final Project project = (Project) rel;
        return MycatProject.create(
                convert(project.getInput(),out),
                project.getProjects(),
                project.getRowType());
    }
}

