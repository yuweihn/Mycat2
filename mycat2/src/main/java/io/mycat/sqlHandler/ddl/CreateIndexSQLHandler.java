package io.mycat.sqlHandler.ddl;

import com.alibaba.fastsql.sql.ast.statement.SQLCreateIndexStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlHandler.AbstractSQLHandler;
import io.mycat.sqlHandler.ExecuteCode;
import io.mycat.sqlHandler.SQLRequest;
import io.mycat.util.Response;




public class CreateIndexSQLHandler extends AbstractSQLHandler<SQLCreateIndexStatement> {

    @Override
    protected ExecuteCode onExecute(SQLRequest<SQLCreateIndexStatement> request, MycatDataContext dataContext, Response response) {
        response.sendOk();
        return ExecuteCode.PERFORMED;
    }
}
