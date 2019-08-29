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

import io.mycat.MycatException;
import io.mycat.beans.mycat.MycatSchema;
import io.mycat.command.loaddata.LoaddataContext;
import io.mycat.command.prepareStatement.PrepareStmtContext;
import io.mycat.config.schema.SchemaType;
import io.mycat.grid.BlockProxyCommandHandler;
import io.mycat.plug.PlugRuntime;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.session.MycatSession;
import io.mycat.router.MycatRouter;
import io.mycat.router.MycatRouterConfig;
import io.mycat.router.ProxyRouteResult;
import io.mycat.sqlparser.util.simpleParser.BufferSQLContext;

/**
 * @author jamie12221 date 2019-05-13 02:47
 **/
public class HybridProxyCommandHandler extends AbstractCommandHandler {

  private MycatRouter router;
  private MycatSession mycat;
  private PrepareStmtContext prepareContext;
  private final LoaddataContext loadDataContext = new LoaddataContext();
  private ProxyQueryHandler proxyQueryHandler;
  private BlockProxyCommandHandler serverQueryHandler;

  @Override
  public void initRuntime(MycatSession mycat, ProxyRuntime runtime) {
    this.mycat = mycat;
    this.router = new MycatRouter((MycatRouterConfig) runtime.getDefContext().get("routerConfig"));
    this.prepareContext = new PrepareStmtContext(mycat);
    this.proxyQueryHandler = new ProxyQueryHandler(router, runtime);
    this.serverQueryHandler = new BlockProxyCommandHandler();
    this.serverQueryHandler.initRuntime(mycat, runtime);
  }

  @Override
  public void handleQuery(byte[] sqlBytes, MycatSession mycat) {
    MycatSchema schema = router.getSchemaBySchemaName(mycat.getSchema());
    if (schema == null) {
      MycatSchema defaultSchema = router.getDefaultSchema();
      if (defaultSchema == null) {
        throw new MycatException("default schema {} is not existed", defaultSchema);
      }
      schema = defaultSchema;
    }
    SchemaType schemaType = schema.getSchemaType();
    if (schemaType == SchemaType.SQL_PARSE_ROUTE) {
      serverQueryHandler.handleQuery(sqlBytes, mycat);
      return;
    }
    proxyQueryHandler.doQuery(schema, sqlBytes, mycat);
  }


  @Override
  public void handleContentOfFilename(byte[] sql, MycatSession session) {
    MycatSchema schema = router.getSchemaOrDefaultBySchemaName(mycat.getSchema());
    if (schema == null) {
      schema = router.getDefaultSchema();
    }
    if (schema.getSchemaType() == SchemaType.DB_IN_ONE_SERVER) {
      this.loadDataContext.append(sql);
    } else {
      mycat.setLastMessage(
          "MySQLProxyPrepareStatement only support in DB_IN_ONE_SERVER");
      mycat.writeErrorEndPacket();
    }
  }

  @Override
  public void handleContentOfFilenameEmptyOk(MycatSession mycat) {
    this.loadDataContext.proxy(mycat);
  }

  @Override
  public void handlePrepareStatement(byte[] bytes, MycatSession mycat) {
    MycatSchema schema = router.getSchemaBySchemaName(mycat.getSchema());
    if (schema == null) {
      mycat.setLastMessage("not select schema");
      mycat.writeErrorEndPacket();
      return;
    }
    String sql = new String(bytes);
    BufferSQLContext bufferSQLContext = router.simpleParse(sql);
    ProxyRouteResult resultRoute = router.enterRoute(schema, bufferSQLContext, sql);
    if (schema.getSchemaType() != SchemaType.DB_IN_ONE_SERVER) {
      mycat.setLastMessage(
          "MySQLProxyPrepareStatement only support in DB_IN_ONE_SERVER");
      mycat.writeErrorEndPacket();
      return;
    }
    ProxyRouteResult route = resultRoute;
    LoadBalanceStrategy balance = PlugRuntime.INSTCANE
        .getLoadBalanceByBalanceName(resultRoute.getBalance());
    String dataNode = schema.getDefaultDataNode();
    mycat.switchDataNode(dataNode);
    prepareContext.newReadyPrepareStmt(sql, dataNode, route.isRunOnMaster(true), balance);
    return;

  }

  @Override
  public void handlePrepareStatementLongdata(long statementId, int paramId, byte[] data,
      MycatSession mycat) {
    MycatSchema schema = router.getSchemaBySchemaName(mycat.getSchema());
    if (schema == null) {
      mycat.setLastMessage("not select schema");
      mycat.writeErrorEndPacket();
      return;
    }

    if (schema.getSchemaType() == SchemaType.DB_IN_ONE_SERVER) {
      prepareContext.appendLongData(statementId, paramId, data);
    } else {
      mycat.setLastMessage(
          "MySQLProxyPrepareStatement only support in DB_IN_ONE_SERVER");
      mycat.writeErrorEndPacket();
    }
  }

  @Override
  public void handlePrepareStatementExecute(byte[] rawPayload, long statementId, byte flags,
      int numParams,
      byte[] rest,
      MycatSession mycat) {
    prepareContext.execute(statementId, flags, numParams, rest, mycat.getDataNode(), true, null);
  }


  @Override
  public void handlePrepareStatementClose(long statementId, MycatSession session) {
    prepareContext.close(statementId);
  }

  @Override
  public void handlePrepareStatementFetch(long statementId, long row, MycatSession mycat) {
    prepareContext.fetch(statementId, row);
  }

  @Override
  public void handlePrepareStatementReset(long statementId, MycatSession session) {
    prepareContext.reset(statementId);
  }

  @Override
  public int getNumParamsByStatementId(long statementId, MycatSession mycat) {
    return prepareContext.getNumOfParams(statementId);
  }
}
