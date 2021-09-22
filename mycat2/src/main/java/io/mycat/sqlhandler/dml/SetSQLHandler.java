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
package io.mycat.sqlhandler.dml;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.visitor.MycatSQLEvalVisitorUtils;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.druid.sql.ast.statement.SQLSetStatement;
import io.mycat.MySQLVariablesUtil;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;
import io.vertx.core.Future;

import java.util.Collections;
import java.util.List;


public class SetSQLHandler extends AbstractSQLHandler<SQLSetStatement> {

    static enum VarType {
        USER,
        SESSION,
        GLOABL
    }

    @Override
    protected Future<Void> onExecute(SQLRequest<SQLSetStatement> request, MycatDataContext dataContext, Response response){
        try {
            List<SQLAssignItem> items = request.getAst().getItems();
            if (items == null) {
                items = Collections.emptyList();
            }

            for (SQLAssignItem item : items) {
                VarType varType = VarType.SESSION;
                String name;
                if (item.getTarget() instanceof SQLPropertyExpr) {
                    SQLPropertyExpr sqlPropertyExpr = (SQLPropertyExpr) item.getTarget();
                    SQLExpr owner = sqlPropertyExpr.getOwner();
                    name = SQLUtils.normalize(sqlPropertyExpr.getSimpleName());
                    if (owner instanceof SQLVariantRefExpr) {
                        varType = ((SQLVariantRefExpr) owner).isGlobal() ? VarType.GLOABL : VarType.SESSION;
                    }
                } else {
                    name = SQLUtils.normalize(item.getTarget().toString());
                    if (name.startsWith("@@")) {
                        name = name.substring(2);
                        varType = VarType.SESSION;
                    }
                    if (name.startsWith("@")) {
                        name = name.substring(1);
                        varType = VarType.USER;
                    }
                }
                if (varType == VarType.GLOABL) {
                    throw new IllegalArgumentException("unsupported set global variables:" + item);
                }
                SQLExpr valueExpr = item.getValue();
                Object value;
                if (valueExpr instanceof SQLNullExpr) {
                    value = null;
                } else {
                    if(valueExpr instanceof SQLIdentifierExpr){
                        value=SQLUtils.normalize(((SQLIdentifierExpr) valueExpr).getSimpleName());
                    }else if(valueExpr instanceof SQLDefaultExpr) {
                        //todo
                        value = "default";
                    }else {
                        value = MycatSQLEvalVisitorUtils.eval(DbType.mysql, valueExpr);
                    }
                }
                switch (varType) {
                    case SESSION: {
                        if ("autocommit".equalsIgnoreCase(name)) {
                            int i = MySQLVariablesUtil.toInt(value);
                            if (i == 0) {
                                if (dataContext.isInTransaction()) {
                                    dataContext.setAutoCommit(false);
                                    return response.sendOk();
                                } else {
                                    dataContext.setAutoCommit(false);
                                    return response.sendOk();
                                }
                            } else if (i == 1) {
                                if (dataContext.isInTransaction()) {
                                    dataContext.setAutoCommit(true);
                                    return response.commit();
                                } else {
                                    dataContext.setAutoCommit(true);
                                    return response.sendOk();
                                }
                            }
                        }
                        dataContext.setVariable(name, item.getValue());
                        return response.sendOk();
                    }
                    case USER:
                    case GLOABL:
                    default:
                        return response.sendError(new IllegalStateException("Unexpected value: " + varType));
                }
            }
            return response.sendOk();
        }catch (Throwable throwable){
            return response.sendError(throwable);
        }
    }

    public SetSQLHandler() {
    }

    public SetSQLHandler(Class statementClass) {
        super(statementClass);
    }
}