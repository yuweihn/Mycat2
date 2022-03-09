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

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLShowColumnsStatement;
import com.alibaba.druid.util.JdbcUtils;
import io.mycat.*;
import io.mycat.calcite.DrdsRunnerHelper;
import io.mycat.calcite.table.GlobalTable;
import io.mycat.calcite.table.NormalTable;
import io.mycat.calcite.table.ShardingTable;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.prototypeserver.mysql.HackRouter;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.util.MycatSQLExprTableSourceUtil;
import io.vertx.core.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


/**
 * @ chenjunwen
 */

public class ShowColumnsSQLHandler extends AbstractSQLHandler<SQLShowColumnsStatement> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ShowColumnsSQLHandler.class);

    @Override
    protected Future<Void> onExecute(SQLRequest<SQLShowColumnsStatement> request, MycatDataContext dataContext, Response response) {
        SQLShowColumnsStatement ast = request.getAst();
        if (ast.getDatabase() == null && dataContext.getDefaultSchema() != null) {
            ast.setDatabase(new SQLIdentifierExpr(dataContext.getDefaultSchema()));
        }
        try {
            JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
            String database = SQLUtils.normalize(ast.getDatabase().getSimpleName());
            String table = SQLUtils.normalize(ast.getTable().getSimpleName());
            MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
            TableHandler tableHandler = metadataManager.getTable(database, table);

            boolean okOnPrototype = false;
            try (DefaultConnection connection = jdbcConnectionManager.getConnection(MetadataManager.getPrototype())) {
                JdbcUtils.executeQuery(connection.getRawConnection(), ast.toString(), Collections.emptyList());
                okOnPrototype = true;
            } catch (Throwable throwable) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("try query {} from prototype fail", ast);
                }
            }
            if (okOnPrototype) {
                return response.proxySelect(Collections.singletonList(MetadataManager.getPrototype()), ast.toString(), Collections.emptyList());
            }
            Partition dataNode = null;
            boolean okOnDataNode = false;
            if (tableHandler.getType() == LogicTableType.NORMAL) {
                dataNode = ((NormalTable) tableHandler).getDataNode();
            } else if (tableHandler.getType() == LogicTableType.GLOBAL) {
                dataNode = ((GlobalTable) tableHandler).getDataNode();
            } else if (tableHandler.getType() == LogicTableType.SHARDING) {
                dataNode = ((ShardingTable) tableHandler).getBackends().get(0);
            }
            if (dataNode != null) {
                SQLShowColumnsStatement tryAst = (SQLShowColumnsStatement) ast.clone();
                tryAst.setTable(new SQLIdentifierExpr("`" + dataNode.getTable() + "`"));
                tryAst.setDatabase(new SQLIdentifierExpr("`" + dataNode.getSchema() + "`"));

                try (DefaultConnection connection = jdbcConnectionManager.getConnection(dataNode.getTargetName())) {
                    JdbcUtils.executeQuery(connection.getRawConnection(), ast.toString(), Collections.emptyList());
                    okOnDataNode = true;
                } catch (Throwable throwable) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("try query {} from partition fail", ast);
                    }
                }
                if (okOnDataNode) {
                    return response.proxySelect(Collections.singletonList(dataNode.getTargetName()), ast.toString(), Collections.emptyList());
                }
            }
        } catch (Exception e) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("try query {} from ds fail", ast);
            }
        }
        String sql = toNormalSQL(request.getAst());
        return DrdsRunnerHelper.runOnDrds(dataContext, DrdsRunnerHelper.preParse(sql, dataContext.getDefaultSchema()), response);

    }

    private String toNormalSQL(SQLShowColumnsStatement ast) {

        boolean full = ast.isFull();

        SQLName table = ast.getTable();
        SQLName database = ast.getDatabase();
        SQLExpr like = ast.getLike();
        SQLExpr where = ast.getWhere();
        List<String[]> list = full ?
                Arrays.asList(
                        new String[]{"COLUMN_NAME", "Field"},
                        new String[]{"COLUMN_TYPE", "Type"},//varchar(64)
                        new String[]{"COLLATION_NAME", "Collation"},//utf8_tolower_ci
                        new String[]{"IS_NULLABLE", "Null"},//YES
                        new String[]{"COLUMN_KEY", "Key"},//""
                        new String[]{"COLUMN_DEFAULT", "Default"},//null
                        new String[]{"EXTRA", "Extra"},//""
                        new String[]{"PRIVILEGES", "Privileges"},//SELECT...
                        new String[]{"COLUMN_COMMENT", "Comment"}//""
                ) : Arrays.asList(
                new String[]{"COLUMN_NAME", "Field"},
                new String[]{"COLUMN_TYPE", "Type"},//varchar(64)
                new String[]{"COLLATION_NAME", "Collation"},//utf8_tolower_ci
                new String[]{"IS_NULLABLE", "Null"},//YES
                new String[]{"COLUMN_KEY", "Key"},//""
                new String[]{"COLUMN_DEFAULT", "Default"},//null
                new String[]{"EXTRA", "Extra"}//""
        );
        return generateSimpleSQL(list, "information_schema", "COLUMNS",
                "TABLE_NAME = '" + SQLUtils.normalize(table.getSimpleName()) + "' and " + " TABLE_SCHEMA = '" + SQLUtils.normalize(database.getSimpleName()) + "'",
                Optional.ofNullable(where).map(i -> i.toString()).orElse(null),
                Optional.ofNullable(like).map(i -> "COLUMN_NAME like " + i).orElse(null))
                .toString();
    }
}
