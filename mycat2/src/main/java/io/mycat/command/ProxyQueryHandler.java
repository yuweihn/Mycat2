/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.command;

import static io.mycat.sqlparser.util.BufferSQLContext.DESCRIBE_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.LOAD_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SELECT_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SELECT_VARIABLES;
import static io.mycat.sqlparser.util.BufferSQLContext.SET_AUTOCOMMIT_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SET_CHARSET;
import static io.mycat.sqlparser.util.BufferSQLContext.SET_CHARSET_RESULT;
import static io.mycat.sqlparser.util.BufferSQLContext.SET_NET_WRITE_TIMEOUT;
import static io.mycat.sqlparser.util.BufferSQLContext.SET_SQL_SELECT_LIMIT;
import static io.mycat.sqlparser.util.BufferSQLContext.SET_TRANSACTION_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SHOW_DB_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SHOW_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SHOW_TB_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SHOW_VARIABLES_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SHOW_WARNINGS;
import static io.mycat.sqlparser.util.BufferSQLContext.USE_SQL;

import io.mycat.MycatException;
import io.mycat.beans.mycat.MycatSchema;
import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLFieldsType;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.beans.mysql.MySQLIsolationLevel;
import io.mycat.config.schema.SchemaType;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.MySQLPacketUtil;
import io.mycat.proxy.MySQLTaskUtil;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.handler.ResponseType;
import io.mycat.proxy.handler.backend.MySQLDataSourceQuery;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.session.MycatSession;
import io.mycat.router.MycatRouter;
import io.mycat.router.MycatRouterConfig;
import io.mycat.router.ResultRoute;
import io.mycat.router.routeResult.OneServerResultRoute;
import io.mycat.router.routeResult.ResultRouteType;
import io.mycat.router.util.RouterUtil;
import io.mycat.security.MycatUser;
import io.mycat.sequenceModifier.ModifyCallback;
import io.mycat.sequenceModifier.SequenceModifier;
import io.mycat.sqlparser.util.BufferSQLContext;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.Set;

/**
 * @author jamie12221 date 2019-05-17 17:37
 **/
public class ProxyQueryHandler {

  static final MycatLogger LOGGER = MycatLoggerFactory.getLogger(ProxyQueryHandler.class);
  final MycatRouter router;
  final private ProxyRuntime runtime;

  public ProxyQueryHandler(MycatRouter router, ProxyRuntime runtime) {
    this.router = router;
    this.runtime = runtime;
  }

  public void doQuery(MycatSchema schema, byte[] sqlBytes, MycatSession mycat) {
    /**
     * 获取默认的schema
     */
    MycatSchema useSchema = schema;
    if (useSchema == null) {
      useSchema = router.getDefaultSchema();
    }
    MycatUser user = mycat.getUser();
    String orgin = new String(sqlBytes);
    MycatMonitor.onOrginSQL(mycat, orgin);
    String sql = RouterUtil.removeSchema(orgin, useSchema.getSchemaName());
    BufferSQLContext sqlContext = router.simpleParse(sql);
    sql = sql.trim();

    byte sqlType = sqlContext.getSQLType();
    if (mycat.isBindMySQLSession()) {
      MySQLTaskUtil.proxyBackend(mycat, MySQLPacketUtil.generateComQuery(sql),
          mycat.getMySQLSession().getDataNode().getName(), null, ResponseType.QUERY);
      return;
    }
    try {
      switch (sqlType) {
        case USE_SQL: {
          String schemaName = sqlContext.getSchemaName(0);
          useSchema(mycat, schemaName);
          break;
        }
        case SET_AUTOCOMMIT_SQL: {
          Boolean autocommit = sqlContext.isAutocommit();
          if (autocommit == null) {
            mycat.setLastMessage("set autocommit fail!");
            mycat.writeErrorEndPacket();
            return;
          } else {
            mycat.setAutoCommit(autocommit ? MySQLAutoCommit.ON : MySQLAutoCommit.OFF);
            mycat.writeOkEndPacket();
            return;
          }
        }
        case SET_CHARSET: {
          String charset = sqlContext.getCharset();
          mycat.setCharset(charset);
          mycat.writeOkEndPacket();
          return;
        }
        case SET_SQL_SELECT_LIMIT: {
          mycat.setSelectLimit(sqlContext.getSqlSelectLimit());
          mycat.writeOkEndPacket();
          return;
        }
        case SET_NET_WRITE_TIMEOUT: {
          mycat.setNetWriteTimeout(sqlContext.getNetWriteTimeout());
          mycat.writeOkEndPacket();
          return;
        }
        case SET_CHARSET_RESULT: {
          String charsetSetResult = sqlContext.getCharsetSetResult();
          mycat.setCharsetSetResult(charsetSetResult);
          mycat.writeOkEndPacket();
          return;
        }
        case SET_TRANSACTION_SQL: {
          if (sqlContext.isAccessMode()) {
            mycat.setAccessModeReadOnly(true);
            mycat.writeOkEndPacket();
            return;
          }
          if (sqlContext.getTransactionLevel() == MySQLIsolationLevel.GLOBAL) {
            LOGGER.warn("unsupport global send error", sql);
            mycat.setLastMessage("unsupport global level");
            mycat.writeErrorEndPacket();
            return;
          }
          MySQLIsolation isolation = sqlContext.getIsolation();
          if (isolation == null) {
            mycat.setLastMessage("set transaction fail!");
            mycat.writeErrorEndPacket();
            return;
          }
          mycat.setIsolation(isolation);
          mycat.writeOkEndPacket();
          return;
        }
        case SHOW_DB_SQL: {
          MycatRouterConfig config = router.getConfig();
          showDb(mycat, config.getSchemaList());
          break;
        }
        case SHOW_TB_SQL: {
          String schemaName =
              sqlContext.getSchemaCount() == 1 ? sqlContext.getSchemaName(0)
                  : useSchema.getSchemaName();
          showTable(mycat, schemaName);
          break;
        }
        case DESCRIBE_SQL:
//          mycat.setLastMessage("unsupport desc");
//          mycat.writeErrorEndPacket();
//          return;
        case SHOW_SQL:
          String defaultDataNode = useSchema.getDefaultDataNode();
          if (defaultDataNode == null) {
            throw new MycatException("show sql:{} can not route", sql);
          }
          MySQLTaskUtil
              .proxyBackend(mycat, MySQLPacketUtil.generateComQuery(sql), defaultDataNode, null,
                  ResponseType.QUERY);
          return;
        case SHOW_VARIABLES_SQL: {
          mycat.writeColumnCount(2);
          mycat.writeColumnDef("Variable_name", MySQLFieldsType.FIELD_TYPE_VAR_STRING);
          mycat.writeColumnDef("Value", MySQLFieldsType.FIELD_TYPE_VAR_STRING);
          mycat.writeColumnEndPacket();

          Set<Entry<String, String>> entries = runtime.getVariables().entries();
          for (Entry<String, String> entry : entries) {
            mycat.writeTextRowPacket(
                new byte[][]{mycat.encode(entry.getKey()), mycat.encode(entry.getValue())});
          }
          mycat.writeRowEndPacket(false, false);
          return;
        }

        case SHOW_WARNINGS: {
          mycat.writeColumnCount(3);
          mycat.writeColumnDef("Level", MySQLFieldsType.FIELD_TYPE_VAR_STRING);
          mycat.writeColumnDef("Code", MySQLFieldsType.FIELD_TYPE_LONG_BLOB);
          mycat.writeColumnDef("CMessage", MySQLFieldsType.FIELD_TYPE_VAR_STRING);
          mycat.writeColumnEndPacket();
          mycat.writeRowEndPacket(false, false);
          return;
        }
        case SELECT_VARIABLES: {
          if (sqlContext.isSelectAutocommit()) {
            mycat.writeColumnCount(1);
            mycat.writeColumnDef("@@session.autocommit", MySQLFieldsType.FIELD_TYPE_VAR_STRING);
            mycat.writeColumnEndPacket();
            mycat.writeTextRowPacket(new byte[][]{mycat.encode(mycat.getAutoCommit().getText())});
            mycat.writeRowEndPacket(false, false);
            return;
          } else if (sqlContext.isSelectTxIsolation()) {
            mycat.writeColumnCount(1);
            mycat.writeColumnDef("@@session.tx_isolation", MySQLFieldsType.FIELD_TYPE_VAR_STRING);
            mycat.writeColumnEndPacket();
            mycat.writeTextRowPacket(new byte[][]{mycat.encode(mycat.getIsolation().getText())});
            mycat.writeRowEndPacket(false, false);
            return;
          } else if (sqlContext.isSelectTranscationReadOnly()) {
            mycat.writeColumnCount(1);
            mycat.writeColumnDef("@@session.transaction_read_only",
                MySQLFieldsType.FIELD_TYPE_LONGLONG);
            mycat.writeColumnEndPacket();
            mycat.writeTextRowPacket(new byte[][]{mycat.encode(mycat.getIsolation().getText())});
            mycat.writeRowEndPacket(false, false);
            return;
          }
          LOGGER.warn("maybe unsupported  sql:{}", sql);
        }
        case LOAD_SQL: {
          LOGGER.warn("Use annotations to specify loadata data nodes whenever possible !");
        }
        case SELECT_SQL:
        default: {
          MycatSchema fSchema = useSchema;
          if (useSchema.getSchemaType() == SchemaType.ANNOTATION_ROUTE) {
            SequenceModifier modifier = useSchema.getModifier();
            if (modifier != null) {
              modifier.modify(fSchema.getSchemaName(), sql, new ModifyCallback() {
                @Override
                public void onSuccessCallback(String sql) {
                  execute(mycat, fSchema, sql, sqlContext, sqlType);
                }

                @Override
                public void onException(Exception e) {
                  mycat.setLastMessage(e);
                  mycat.writeErrorEndPacket();
                }
              });
              return;
            }
          }
          execute(mycat, fSchema, sql, sqlContext, sqlType);
        }
      }
    } catch (Exception e) {
      mycat.setLastMessage(e);
      mycat.writeErrorEndPacket();
    }
  }

  public void execute(MycatSession mycat, MycatSchema useSchema, String sql,
      BufferSQLContext sqlContext, byte sqlType) {
    boolean simpleSelect = sqlContext.isSimpleSelect() && sqlType == SELECT_SQL;
    ResultRoute resultRoute = router.enterRoute(useSchema, sqlContext, sql);
    if (resultRoute == null) {
      mycat.setLastMessage("can not route:" + sql);
      mycat.writeErrorEndPacket();
    } else if (resultRoute.getType() == ResultRouteType.ONE_SERVER_RESULT_ROUTE) {
      OneServerResultRoute resultRoute1 = (OneServerResultRoute) resultRoute;
      MySQLDataSourceQuery query = new MySQLDataSourceQuery();
      query.setIds(null);
      query.setRunOnMaster(resultRoute.isRunOnMaster(!simpleSelect));
      query.setStrategy(runtime
          .getLoadBalanceByBalanceName(resultRoute.getBalance()));
      MySQLTaskUtil
          .proxyBackend(mycat, MySQLPacketUtil.generateComQuery(resultRoute1.getSql()),
              resultRoute1.getDataNode(), query, ResponseType.QUERY);
    } else {
      mycat.setLastMessage("unsupport sql");
      mycat.writeErrorEndPacket();
    }
  }

  public void showDb(MycatSession mycat, Collection<MycatSchema> schemaList) {
    mycat.writeColumnCount(1);
    byte[] bytes = MySQLPacketUtil
        .generateColumnDef("information_schema", "SCHEMATA", "SCHEMATA", "Database",
            "SCHEMA_NAME",
            MySQLFieldsType.FIELD_TYPE_VAR_STRING,
            0x1, 0, mycat.charsetIndex(), 192, Charset.defaultCharset());
    mycat.writeBytes(bytes, false);
    mycat.writeColumnEndPacket();
    for (MycatSchema schema : schemaList) {
      String schemaName = schema.getSchemaName();
      mycat.writeTextRowPacket(new byte[][]{schemaName.getBytes(mycat.charset())});
    }
    mycat.countDownResultSet();
    mycat.writeRowEndPacket(mycat.hasResultset(), mycat.hasCursor());
  }

  public void showTable(MycatSession mycat, String schemaName) {
    Collection<String> tableName = router.getConfig().getSchemaBySchemaName(schemaName)
        .getMycatTables().keySet();
    mycat.writeColumnCount(2);
    mycat.writeColumnDef("Tables in " + tableName, MySQLFieldsType.FIELD_TYPE_VAR_STRING);
    mycat.writeColumnDef("Table_type " + tableName, MySQLFieldsType.FIELD_TYPE_VAR_STRING);
    mycat.writeColumnEndPacket();
    MycatRouterConfig config = router.getConfig();
    MycatSchema schema = config.getSchemaBySchemaName(schemaName);
    byte[] basetable = mycat.encode("BASE TABLE");
    for (String name : schema.getMycatTables().keySet()) {
      mycat.writeTextRowPacket(new byte[][]{mycat.encode(name), basetable});
    }
    mycat.writeRowEndPacket(mycat.hasResultset(), mycat.hasCursor());
  }

  public void useSchema(MycatSession mycat, String schemaName) {
    mycat.useSchema(schemaName);
    mycat.writeOkEndPacket();
  }
}
