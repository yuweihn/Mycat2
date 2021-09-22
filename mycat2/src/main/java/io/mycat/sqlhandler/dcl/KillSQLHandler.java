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
package io.mycat.sqlhandler.dcl;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlKillStatement;
import io.mycat.*;
import io.mycat.Process;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.vertx.core.Future;

import java.util.ArrayList;
import java.util.List;


public class KillSQLHandler extends AbstractSQLHandler<MySqlKillStatement> {

    @Override
    protected Future<Void> onExecute(SQLRequest<MySqlKillStatement> request, MycatDataContext dataContext, Response response) {
        MycatServer mycatServer = MetaClusterCurrent.wrapper(MycatServer.class);
        MySqlKillStatement ast = request.getAst();
        ArrayList<Long> ids = new ArrayList<>();
        for (SQLExpr threadId : ast.getThreadIds()) {
            ids.add( Long.parseLong(SQLUtils.normalize(threadId.toString())));
        }
        return response.sendOk(mycatServer.kill(ids));
    }
}
