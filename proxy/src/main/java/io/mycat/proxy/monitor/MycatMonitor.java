/**
 * Copyright (C) <2020>  <chen junwen>
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
package io.mycat.proxy.monitor;

import io.mycat.proxy.handler.backend.MySQLSynContext;
import io.mycat.proxy.packet.MySQLPayloadType;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MycatSession;
import io.mycat.proxy.session.Session;

import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Objects;

import static io.mycat.proxy.monitor.MycatMonitorCallback.EMPTY;

/**
 * @author jamie12221 date 2019-05-14 11:51
 **/
public final class MycatMonitor {

  static MycatMonitorCallback callback = EMPTY;

  public final static void onOrginSQL(Session session, String sql) {
    Objects.requireNonNull(session);
    Objects.requireNonNull(sql);
    callback.onOrginSQL(session, sql);
  }


  public final static void onRouteSQL(Session session, String replicaName,String defaultDatabase,String sql){
    Objects.requireNonNull(session);
    Objects.requireNonNull(replicaName);
    Objects.requireNonNull(defaultDatabase);
    Objects.requireNonNull(sql);
    callback.onRouteSQL(session, replicaName,defaultDatabase,sql);
  }

  public final static void onRouteResult(Session session, String replicaName, String defaultDatabase,
      String dataSourceName,
      String sql) {
    Objects.requireNonNull(session);
    Objects.requireNonNull(replicaName);
    Objects.requireNonNull(defaultDatabase);
    Objects.requireNonNull(dataSourceName);
    callback.onRouteSQLResult(session, replicaName, defaultDatabase, dataSourceName, sql);
  }

  public final static void onRouteResult(Session session, String dataNodeName, String replicaName,
      String dataSourceName,
      byte[] packet) {
    Objects.requireNonNull(session);
    Objects.requireNonNull(dataNodeName);
    Objects.requireNonNull(replicaName);
    Objects.requireNonNull(dataSourceName);
    callback.onRouteSQLResult(session, dataNodeName, replicaName, dataSourceName, packet);
  }

  public static void setCallback(MycatMonitorCallback callback) {
    Objects.requireNonNull(callback);
    MycatMonitor.callback = callback;
  }

  public static void onPayloadType(Session session, MySQLPayloadType type
//      , ByteBuffer view,
//      int startIndex, int len
  ) {
    callback.onPayloadType(session, type
//        , view, startIndex, len
    );
  }

  public final static void onFrontRead(Session session, ByteBuffer view, int startIndex, int len) {
    callback.onFrontRead(session, view, startIndex, len);
  }

  public final static void onBackendWrite(Session session, ByteBuffer view, int startIndex,
      int len) {
    callback.onBackendWrite(session, view, startIndex, len);
  }

  public final static void onBackendRead(Session session, ByteBuffer view, int startIndex,
      int len) {
    callback.onBackendRead(session, view, startIndex, len);
  }

  public final static void onFrontWrite(Session session, ByteBuffer view, int startIndex, int len) {
    callback.onFrontWrite(session, view, startIndex, len);
  }

  public final static void onMySQLSessionServerStatusChanged(Session session, int serverStatus) {
    callback.onMySQLSessionServerStatusChanged(session, serverStatus);
  }

  public final static void onSynchronizationState(MySQLClientSession session) {
    callback.onSynchronizationState(session);
  }

  public final static void onAllocateByteBuffer(ByteBuffer buffer,Session session) {
    callback.onAllocateByteBuffer(buffer,session);
  }

  public final static void onRecycleByteBuffer(ByteBuffer buffer,Session session) {
    callback.onRecycleByteBuffer(buffer,session);
  }

  public final static void onExpandByteBuffer(ByteBuffer buffer,Session session) {
    callback.onExpandByteBuffer(buffer,session);
  }

  public final static void onNewMycatSession(MycatSession session) {
    callback.onNewMycatSession(session);
  }

  public final static void onBindMySQLSession(MycatSession mycat, MySQLClientSession session) {
    callback.onBindMySQLSession(mycat, session);
  }

  public final static void onUnBindMySQLSession(MycatSession mycat, MySQLClientSession session) {
    callback.onUnBindMySQLSession(mycat, session);
  }

  public final static void onNewMySQLSession(MySQLClientSession session) {
    callback.onNewMySQLSession(session);
  }

  public final static void onAddIdleMysqlSession(MySQLClientSession session) {
    callback.onAddIdleMysqlSession(session);
  }

  public final static void onGetIdleMysqlSession(MySQLClientSession session) {
    callback.onGetIdleMysqlSession(session);
  }

  public final static void onCloseMysqlSession(MySQLClientSession session, boolean normal,
      String hint) {
    callback.onCloseMysqlSession(session, normal, hint);
  }

  /**
   * exception
   */
  public final static void onBackendConCreateWriteException(Session session, Exception e) {
    callback.onBackendConCreateWriteException(session, e);
  }

  public final static void onBackendConCreateReadException(Session session, Exception e) {
    callback.onBackendConCreateReadException(session, e);
  }

  public final static void onBackendConCreateConnectException(Session session, Exception e) {
    callback.onBackendConCreateConnectException(session, e);
  }

  public final static void onBackendConCreateException(Session session, Exception e) {
    callback.onBackendConCreateException(session, e);
  }

  public final static void onIdleReadException(Session session, Exception e) {
    callback.onIdleReadException(session, e);
  }

  public final static void onBackendConCreateClear(Session session) {
    callback.onBackendConCreateClear(session);
  }

  public final static void onBackendResultSetReadException(Session session, Exception e) {
    callback.onBackendResultSetReadException(session, e);
  }

  public final static void onBackendResultSetWriteException(Session session, Exception e) {
    callback.onBackendResultSetWriteException(session, e);
  }

  public final static void onBackendResultSetWriteClear(Session session, Exception e) {
    callback.onBackendResultSetWriteClear(session);
  }

  public final static void onResultSetWriteException(Session session, Exception e) {
    callback.onResultSetWriteException(session, e);
  }

  public final static void onResultSetReadException(Session session, Exception e) {
    callback.onResultSetReadException(session, e);
  }

  public final static void onResultSetClear(Session session) {
    callback.onResultSetClear(session);
  }

  public final static void onAuthHandlerWriteException(Session session, Exception e) {
    callback.onAuthHandlerWriteException(session, e);
  }

  public final static void onAuthHandlerReadException(Session session, Exception e) {
    callback.onAuthHandlerReadException(session, e);
  }

  public final static void onAuthHandlerClear(Session session) {
    callback.onAuthHandlerClear(session);
  }

  public final static void onPacketExchangerWriteException(Session session, Exception e) {
    callback.onPacketExchangerWriteException(session, e);
  }

  public final static void onPacketExchangerException(Session session, Exception e) {
    callback.onPacketExchangerException(session, e);
  }

  public final static void onPacketExchangerRead(Session session) {
    callback.onPacketExchangerRead(session);
  }

  public final static void onPacketExchangerWrite(Session session) {
    callback.onPacketExchangerWrite(session);
  }

  public final static void onPacketExchangerClear(Session session) {
    callback.onPacketExchangerClear(session);
  }

  public final static void onGettingBackendException(Session session, String replicaName,String defaultDatabase,
      Exception e) {
    callback.onGettingBackend(session, replicaName,defaultDatabase, e);
  }

  public final static void onMycatHandlerWriteException(Session session, Exception e) {
    callback.onMycatHandlerWriteException(session, e);
  }

  public final static void onMycatHandlerClear(Session session) {
    callback.onMycatHandlerClear(session);
  }

  public static void onRequestException(MySQLClientSession session, Exception e) {
    callback.onRequestException(session, e);
  }

  public static void onRequestClear(MySQLClientSession session) {
    callback.onRequestClear(session);
  }

  public static void onResultSetException(MySQLClientSession session, Exception e) {
    callback.onResultSetException(session, e);
  }

  public static void onAuthHandlerException(MycatSession session, Exception e) {
    callback.onAuthHandlerReadException(session, e);
  }

  public static void onMycatHandlerReadException(MycatSession mycat, Exception e) {
    callback.onMycatHandlerReadException(mycat, e);
  }

  public static void onMycatHandlerException(MycatSession mycat, Exception e) {
    callback.onMycatHandlerException(mycat, e);
  }

  public static void onMycatHandlerCloseException(MycatSession mycat, ClosedChannelException e) {
    callback.onMycatHandlerCloseException(mycat, e);
  }

  public static void onMycatServerWriteException(MycatSession session, Exception e) {
    callback.onMycatServerWriteException(session, e);
  }

  public static void onCloseMycatSession(MycatSession mycat, boolean normal, String reason) {
    callback.onCloseMycatSession(mycat, normal, reason);
  }

  //command
  public static void onCommandStart(MycatSession mycat) {
    callback.onCommandStart(mycat);
  }

  public static void onCommandEnd(MycatSession mycat) {
    callback.onCommandEnd(mycat);
  }

  public static void onSleepCommandStart(MycatSession mycat) {
    callback.onSleepCommandStart(mycat);
  }

  public static void onSleepCommandEnd(MycatSession mycat) {
    callback.onSleepCommandEnd(mycat);
  }

  public static void onQuitCommandStart(MycatSession mycat) {
    callback.onQuitCommandStart(mycat);
  }

  public static void onQuitCommandEnd(MycatSession mycat) {
    callback.onQuitCommandEnd(mycat);
  }

  public static void onQueryCommandStart(MycatSession mycat) {
    callback.onQueryCommandStart(mycat);
  }

  public static void onQueryCommandEnd(MycatSession mycat) {
    callback.onQueryCommandEnd(mycat);
  }

  public static void onInitDbCommandStart(MycatSession mycat) {
    callback.onInitDbCommandStart(mycat);
  }

  public static void onInitDbCommandEnd(MycatSession mycat) {
    callback.onInitDbCommandEnd(mycat);
  }

  public static void onPingCommandStart(MycatSession mycat) {
    callback.onPingCommandStart(mycat);
  }

  public static void onPingCommandEnd(MycatSession mycat) {
    callback.onPingCommandEnd(mycat);
  }

  public static void onFieldListCommandStart(MycatSession mycat) {
    callback.onFieldListCommandStart(mycat);
  }

  public static void onFieldListCommandEnd(MycatSession mycat) {
    callback.onFieldListCommandEnd(mycat);
  }

  public static void onSetOptionCommandStart(MycatSession mycat) {
    callback.onSetOptionCommandStart(mycat);
  }

  public static void onSetOptionCommandEnd(MycatSession mycat) {
    callback.onSetOptionCommandEnd(mycat);
  }

  public static void onPrepareCommandStart(MycatSession mycat) {
    callback.onPrepareCommandStart(mycat);
  }

  public static void onPrepareCommandEnd(MycatSession mycat) {
    callback.onPrepareCommandEnd(mycat);
  }

  public static void onSendLongDataCommandStart(MycatSession mycat) {
    callback.onSendLongDataCommandStart(mycat);
  }

  public static void onSendLongDataCommandEnd(MycatSession mycat) {
    callback.onSendLongDataCommandEnd(mycat);
  }

  public static void onExecuteCommandStart(MycatSession mycat) {
    callback.onExecuteCommandStart(mycat);
  }

  public static void onExecuteCommandEnd(MycatSession mycat) {
    callback.onExecuteCommandEnd(mycat);
  }

  public static void onCloseCommandStart(MycatSession mycat) {
    callback.onCloseCommandStart(mycat);
  }

  public static void onCloseCommandEnd(MycatSession mycat) {
    callback.onCloseCommandEnd(mycat);
  }

  public static void onResetCommandStart(MycatSession mycat) {
    callback.onResetCommandStart(mycat);
  }

  public static void onResetCommandEnd(MycatSession mycat) {
    callback.onResetCommandEnd(mycat);
  }

  public static void onCreateDbCommandStart(MycatSession mycat) {
    callback.onCreateDbCommandStart(mycat);
  }

  public static void onCreateDbCommandEnd(MycatSession mycat) {
    callback.onCreateDbCommandEnd(mycat);
  }


  public static void onDropDbCommandStart(MycatSession mycat) {
    callback.onDropDbCommandStart(mycat);
  }

  public static void onDropDbCommandEnd(MycatSession mycat) {
    callback.onDropDbCommandEnd(mycat);
  }

  public static void onRefreshCommandStart(MycatSession mycat) {
    callback.onRefreshCommandStart(mycat);
  }

  public static void onRefreshCommandEnd(MycatSession mycat) {
    callback.onRefreshCommandEnd(mycat);
  }

  public static void onShutdownCommandStart(MycatSession mycat) {
    callback.onShutdownCommandStart(mycat);
  }

  public static void onShutdownCommandEnd(MycatSession mycat) {
    callback.onShutdownCommandEnd(mycat);
  }

  public static void onStatisticsCommandStart(MycatSession mycat) {
    callback.onStatisticsCommandStart(mycat);
  }

  public static void onStatisticsCommandEnd(MycatSession mycat) {
    callback.onStatisticsCommandEnd(mycat);
  }

  public static void onProcessInfoCommandStart(MycatSession mycat) {
    callback.onProcessInfoCommandStart(mycat);
  }

  public static void onProcessInfoCommandEnd(MycatSession mycat) {
    callback.onProcessInfoCommandEnd(mycat);
  }

  public static void onConnectCommandStart(MycatSession mycat) {
    callback.onConnectCommandStart(mycat);
  }

  public static void onConnectCommandEnd(MycatSession mycat) {
    callback.onConnectCommandEnd(mycat);
  }

  public static void onProcessKillCommandStart(MycatSession mycat) {
    callback.onProcessKillCommandStart(mycat);
  }

  public static void onProcessKillCommandEnd(MycatSession mycat) {
    callback.onProcessKillCommandEnd(mycat);
  }

  public static void onDebugCommandStart(MycatSession mycat) {
    callback.onDebugCommandStart(mycat);
  }

  public static void onDebugCommandEnd(MycatSession mycat) {
    callback.onDebugCommandEnd(mycat);
  }

  public static void onTimeCommandStart(MycatSession mycat) {
    callback.onTimeCommandStart(mycat);
  }

  public static void onTimeCommandEnd(MycatSession mycat) {
    callback.onTimeCommandEnd(mycat);
  }

  public static void onDelayedInsertCommandStart(MycatSession mycat) {
    callback.onDelayedInsertCommandStart(mycat);
  }

  public static void onDelayedInsertCommandEnd(MycatSession mycat) {
    callback.onDelayedInsertCommandEnd(mycat);
  }

  public static void onChangeUserCommandStart(MycatSession mycat) {
    callback.onChangeUserCommandStart(mycat);
  }

  public static void onChangeUserCommandEnd(MycatSession mycat) {
    callback.onChangeUserCommandEnd(mycat);
  }

  public static void onResetConnectionCommandStart(MycatSession mycat) {
    callback.onResetConnectionCommandStart(mycat);
  }

  public static void onResetConnectionCommandEnd(MycatSession mycat) {
    callback.onResetConnectionCommandEnd(mycat);
  }

  public static void onDaemonCommandStart(MycatSession mycat) {
    callback.onDaemonCommandStart(mycat);
  }

  public static void onDaemonCommandEnd(MycatSession mycat) {
    callback.onDaemonCommandEnd(mycat);
  }

  public static void onFetchCommandStart(MycatSession mycat) {
    callback.onFetchCommandStart(mycat);
  }

  public static void onFetchCommandEnd(MycatSession mycat) {
    callback.onFetchCommandEnd(mycat);
  }

  public static void onLoadDataLocalInFileEmptyPacketStart(MycatSession mycat) {
    callback.onLoadDataLocalInFileEmptyPacketStart(mycat);
  }

  public static void onLoadDataLocalInFileEmptyPacketEnd(MycatSession mycat) {
    callback.onLoadDataLocalInFileEmptyPacketEnd(mycat);
  }

  public static void onLoadDataLocalInFileContextStart(MycatSession mycat) {
    callback.onLoadDataLocalInFileContextStart(mycat);
  }

  public static void onLoadDataLocalInFileContextEnd(MycatSession mycat) {
    callback.onLoadDataLocalInFileContextEnd(mycat);
  }


  public static void onChange2ReadOpts(Session session) {
    callback.onChange2ReadOpts(session);
  }


  public static void onChange2WriteOpts(Session session) {
    callback.onChange2WriteOpts(session);
  }

  public static void onClearReadWriteOpts(Session session) {
    callback.onClearReadWriteOpts(session);
  }

  public static void onSyncSQL(MySQLSynContext mycatContext, String syncSQL,
      MySQLClientSession mysql) {
    callback.onSyncSQL(mycatContext, syncSQL, mysql);
  }

  public static void onResultSetEnd(MySQLClientSession mysql) {
    callback.onResultSetEnd(mysql);
  }

  public static void onRouteSQL(MycatSession mycat, String dataSourceName, String sql) {
    callback.onRouteSQL(mycat, dataSourceName,sql);
  }
}
