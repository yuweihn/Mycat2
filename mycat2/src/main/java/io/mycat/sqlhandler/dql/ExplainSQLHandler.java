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
package io.mycat.sqlhandler.dql;

import com.alibaba.druid.sql.ast.SQLCommentHint;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlExplainStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import io.mycat.DrdsSqlWithParams;
import io.mycat.MycatDataContext;
import io.mycat.Response;
import io.mycat.api.collector.RowIterable;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.calcite.DrdsRunnerHelper;
import io.mycat.calcite.spm.Plan;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.HackRouter;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.sqlhandler.dml.UpdateSQLHandler;
import io.mycat.util.Pair;
import io.vertx.core.Future;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.JDBCType;
import java.util.Arrays;
import java.util.List;


public class ExplainSQLHandler extends AbstractSQLHandler<MySqlExplainStatement> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExplainSQLHandler.class);

    @Override
    @SneakyThrows
    protected Future<Void> onExecute(SQLRequest<MySqlExplainStatement> request, MycatDataContext dataContext, Response response) {
        MySqlExplainStatement explainAst = request.getAst();
        explainAst.accept0(new MySqlASTVisitorAdapter() {
            @Override
            public boolean visit(SQLExprTableSource x) {
                if (x.getSchema() == null) {
                    x.setSchema(dataContext.getDefaultSchema());
                }
                return super.visit(x);
            }
        });
        if (explainAst.isDescribe()) {
            SQLName tableName = explainAst.getTableName();
            if (tableName instanceof SQLIdentifierExpr && dataContext.getDefaultSchema() != null) {
                explainAst.setTableName(new SQLPropertyExpr(new SQLIdentifierExpr(dataContext.getDefaultSchema()), ((SQLIdentifierExpr) tableName).getName()));
            }
            return response.proxySelectToPrototype(explainAst.toString());
        }
        SQLStatement statement = request.getAst().getStatement();
        boolean forUpdate = false;
        if (statement instanceof SQLSelectStatement) {
            forUpdate = ((SQLSelectStatement) explainAst.getStatement()).getSelect().getFirstQueryBlock().isForUpdate();
        }
        ResultSetBuilder builder = ResultSetBuilder.create().addColumnInfo("plan", JDBCType.VARCHAR);
        try {
            HackRouter hackRouter = new HackRouter(statement, dataContext);
            if (hackRouter.analyse()) {
                Pair<String, String> plan = hackRouter.getPlan();
                builder.addObjectRowPayload(Arrays.asList("targetName: " +
                        plan.getKey() + "   sql: " + plan.getValue()));
            } else {
                List<SQLCommentHint> hints = explainAst.getHints();
                if (hints != null) {
                    statement.setHeadHints(hints);
                }
                DrdsSqlWithParams drdsSqlWithParams = DrdsRunnerHelper.preParse(statement, dataContext.getDefaultSchema());
                Plan plan;
                if (statement instanceof SQLInsertStatement
                        ||
                        statement instanceof SQLUpdateStatement
                        ||
                        statement instanceof SQLDeleteStatement) {
                    plan = UpdateSQLHandler.getPlan(drdsSqlWithParams);
                } else {
                    plan = DrdsRunnerHelper.getPlan(drdsSqlWithParams);
                }
                List<String> explain = plan.explain(dataContext, drdsSqlWithParams, true);
                for (String s1 : explain) {
                    builder.addObjectRowPayload(Arrays.asList(s1));
                }
            }

        } catch (Throwable th) {
            LOGGER.error("", th);
            builder.addObjectRowPayload(Arrays.asList(th.toString()));
        }
        return response.sendResultSet(RowIterable.create(builder.build()));
    }
}
