package io.mycat.manager.commands;

import com.alibaba.fastsql.sql.SQLUtils;
import io.mycat.MycatDataContext;
import io.mycat.client.MycatRequest;
import io.mycat.datasource.jdbc.JdbcRuntime;
import io.mycat.util.Response;

public class CloseJdbcDatasourceCommand implements ManageCommand {
    @Override
    public String statement() {
        return "close @@backend.jdbc.name = 'name'";
    }

    @Override
    public String description() {
        return statement();
    }

    @Override
    public void handle(MycatRequest request, MycatDataContext context, Response response) {
        String s = SQLUtils.normalize(request.getText().split("=")[1].trim());
       throw new UnsupportedOperationException();//todo

    }

    @Override
    public boolean run(MycatRequest request, MycatDataContext context, Response response) {
        if (request.getText().startsWith("close @@backend.jdbc.name =")) {
            handle(request, context, response);
            return true;
        }
        return false;
    }
}