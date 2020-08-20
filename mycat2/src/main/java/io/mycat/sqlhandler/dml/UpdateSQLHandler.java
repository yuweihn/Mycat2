package io.mycat.sqlhandler.dml;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import io.mycat.*;
import io.mycat.hbt3.DrdsConfig;
import io.mycat.hbt3.DrdsConst;
import io.mycat.hbt3.DrdsRunner;
import io.mycat.hbt3.DrdsSql;
import io.mycat.hbt4.DatasourceFactory;
import io.mycat.hbt4.DefaultDatasourceFactory;
import io.mycat.hbt4.MycatRel;
import io.mycat.hbt4.PlanCache;
import io.mycat.metadata.MetadataManager;
import io.mycat.metadata.SchemaHandler;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ExecuteCode;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.util.Response;

import java.util.*;

public class UpdateSQLHandler extends AbstractSQLHandler<MySqlUpdateStatement> {

    @Override
    protected ExecuteCode onExecute(SQLRequest<MySqlUpdateStatement> request, MycatDataContext dataContext, Response response) {
        updateHandler(request.getAst(), dataContext, (SQLExprTableSource) request.getAst().getTableSource(), response);
        return ExecuteCode.PERFORMED;
    }

    public static void updateHandler(SQLStatement sqlStatement, MycatDataContext dataContext, SQLExprTableSource tableSource, Response receiver) {
        String schemaName = Optional.ofNullable(tableSource.getSchema() == null ? dataContext.getDefaultSchema() : tableSource.getSchema())
                .map(i-> SQLUtils.normalize(i)).orElse(null);
        String tableName = SQLUtils.normalize(tableSource.getTableName());
        SchemaHandler schemaHandler;
        Optional<Map<String, SchemaHandler>> handlerMapOptional = Optional.ofNullable(MetadataManager.INSTANCE.getSchemaMap());
        Optional<String> targetNameOptional = Optional.ofNullable(RootHelper.INSTANCE)
                .map(i -> i.getConfigProvider())
                .map(i -> i.currentConfig())
                .map(i -> i.getMetadata())
                .map(i -> i.getPrototype())
                .map(i -> i.getTargetName());
        if (!handlerMapOptional.isPresent()) {
            if (targetNameOptional.isPresent()) {
                receiver.proxyUpdate(targetNameOptional.get(), Objects.toString(sqlStatement));
                return;
            } else {
                receiver.sendError(new MycatException("Unable to route:" + sqlStatement));
                return;
            }
        } else {
            Map<String, SchemaHandler> handlerMap = handlerMapOptional.get();
            schemaHandler = Optional.ofNullable(handlerMap.get(schemaName))
                    .orElseGet(() -> {
                        if (dataContext.getDefaultSchema() == null) {
                            throw new MycatException("unknown schema:"+schemaName);//可能schemaName有值,但是值名不是配置的名字
                        }
                        return handlerMap.get(dataContext.getDefaultSchema());
                    });
            if (schemaHandler == null) {
                receiver.sendError(new MycatException("Unable to route:" + sqlStatement));
                return;
            }
        }
        String defaultTargetName = schemaHandler.defaultTargetName();
        Map<String, TableHandler> tableMap = schemaHandler.logicTables();
        TableHandler tableHandler = tableMap.get(tableName);
        ///////////////////////////////common///////////////////////////////
        if (tableHandler == null) {
            receiver.proxyUpdate(defaultTargetName, sqlStatement.toString());
            return;
        }
        DrdsRunners.runOnDrds(dataContext, receiver, sqlStatement);
    }

    @Override
    public ExecuteCode onExplain(SQLRequest<MySqlUpdateStatement> request, MycatDataContext dataContext, Response response) {
        response.setExplainMode(true);
        updateHandler(request.getAst(), dataContext, (SQLExprTableSource) request.getAst().getTableSource(), response);
        return ExecuteCode.PERFORMED;
    }
}
