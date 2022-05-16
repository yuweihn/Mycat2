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
package io.mycat.sqlhandler.ddl;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.statement.SQLDropTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import io.mycat.*;
import io.mycat.config.MycatRouterConfigOps;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ConfigUpdater;
import io.mycat.sqlhandler.SQLRequest;
import io.vertx.core.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;


public class DropTableSQLHandler extends AbstractSQLHandler<SQLDropTableStatement> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DropTableSQLHandler.class);

    @Override
    protected Future<Void> onExecute(SQLRequest<SQLDropTableStatement> request, MycatDataContext dataContext, Response response) {
        LockService lockService = MetaClusterCurrent.wrapper(LockService.class);
        return lockService.lock(DDL_LOCK, new Supplier<Future<Void>>() {
            @Override
            public Future<Void> get() {
                try {
                    SQLDropTableStatement ast = request.getAst();
                    ast.setIfExists(true);
                    List<SQLExprTableSource> tableSources = ast.getTableSources();
                    if (tableSources.size() != 1) {
                        throw new UnsupportedOperationException("unsupported drop multi table :" + tableSources.get(0));
                    }
                    SQLExprTableSource tableSource = ast.getTableSources().get(0);
                    String schema = SQLUtils.normalize(
                            tableSource.getSchema() == null ?
                                    dataContext.getDefaultSchema() : tableSource.getSchema()
                    );
                    String tableName = SQLUtils.normalize(
                            tableSource.getTableName()
                    );
                    MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);

                    TableHandler tableHandler = metadataManager.getTable(schema, tableName);
                    if (tableHandler != null) {
                        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
                        Set<Partition> dataNodes = new HashSet<>(getDataNodes(tableHandler));
                        try {
                            executeOnDataNodes(ast, jdbcConnectionManager, dataNodes, tableSource);
                        } catch (Throwable throwable) {
                            LOGGER.error("", throwable);
                        }
                        try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
                            ops.removeTable(schema, tableName);
                            ops.commit();
                        }
                        executeOnDataNodes(ast, jdbcConnectionManager, dataNodes, tableSource);
                    }
                    return response.sendOk();
                } catch (Throwable throwable) {
                    LOGGER.error("", throwable);
                    return Future.failedFuture(throwable);
                }
            }
        });
    }

    protected void onPhysics(String schema, String tableName) {
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        try (DefaultConnection connection = jdbcConnectionManager.getConnection(metadataManager.getPrototype())) {
            connection.executeUpdate(String.format(
                    "DROP TABLE IF EXISTS %s;",
                    schema + "." + tableName), false);
        } catch (Throwable t) {
            LOGGER.warn("", t);
        }
    }
}
