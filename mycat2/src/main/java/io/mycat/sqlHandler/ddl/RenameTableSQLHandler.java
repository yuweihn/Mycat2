package io.mycat.sqlHandler.ddl;

import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlRenameTableStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlHandler.AbstractSQLHandler;
import io.mycat.sqlHandler.ExecuteCode;
import io.mycat.sqlHandler.SQLRequest;
import io.mycat.util.Response;




public class RenameTableSQLHandler extends AbstractSQLHandler<MySqlRenameTableStatement> {

    @Override
    protected ExecuteCode onExecute(SQLRequest<MySqlRenameTableStatement> request, MycatDataContext dataContext, Response response) {
        response.sendOk();
        return ExecuteCode.PERFORMED;
    }
}
