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
package io.mycat.proxy.handler;

import io.mycat.MycatException;
import io.mycat.beans.MySQLSessionMonopolizeType;
import io.mycat.beans.mysql.packet.ErrorPacketImpl;
import io.mycat.beans.mysql.packet.MySQLPacket;
import io.mycat.beans.mysql.packet.ProxyBuffer;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.MySQLPacketUtil;
import io.mycat.proxy.MySQLTaskUtil;
import io.mycat.proxy.callback.TaskCallBack;
import io.mycat.proxy.handler.MycatHandler.MycatSessionWriteHandler;
import io.mycat.proxy.handler.backend.MySQLDataSourceQuery;
import io.mycat.proxy.handler.backend.SessionSyncCallback;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.packet.MySQLPacketCallback;
import io.mycat.proxy.packet.MySQLPacketResolver;
import io.mycat.proxy.packet.MySQLPayloadType;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MycatSession;
import java.io.IOException;
import java.util.Objects;

/**
 *
 */
public enum MySQLPacketExchanger {
  INSTANCE;

  private static final MycatLogger LOGGER = MycatLoggerFactory
      .getLogger(MySQLPacketExchanger.class);
  public final static PacketExchangerCallback DEFAULT_BACKEND_SESSION_REQUEST_FAILED_CALLBACK = (mycat, e, attr) -> {
    mycat.setLastMessage(e.getMessage());
    mycat.writeErrorEndPacketBySyncInProcessError();
  };

  private static void onExceptionClearCloseInResponse(MycatSession mycat, Exception e) {
    LOGGER.error("{}", e);
    MycatMonitor.onPacketExchangerException(mycat, e);
    MySQLClientSession mysql = mycat.getMySQLSession();
    if (mysql != null) {
      mysql.resetPacket();
      mysql.setCallBack(null);
      mysql.close(false, e);
      mycat.onHandlerFinishedClear();
    }
    mycat.close(false, e);
  }

  private static void onExceptionClearCloseInRequest(MycatSession mycat, Exception e,
      PacketExchangerCallback callback) {
    LOGGER.error("{}", e);
    MycatMonitor.onPacketExchangerWriteException(mycat, e);
    MySQLClientSession mysql = mycat.getMySQLSession();
    if (mysql != null) {
      mysql.setCallBack(null);
      mysql.resetPacket();
      mysql.close(false, e);
    }
    callback.onRequestMySQLException(mycat, e, null);
  }

  private static void onClearInNormalResponse(MycatSession mycatSession, MySQLClientSession mysql) {
    mycatSession.resetPacket();
    mysql.resetPacket();

    if (!mysql.isMonopolized()) {
      mycatSession.setMySQLSession(null);
      mysql.setMycatSession(null);
      MycatMonitor.onUnBindMySQLSession(mycatSession, mysql);
      mysql.switchNioHandler(null);
      mysql.getSessionManager().addIdleSession(mysql);
    }
    mycatSession.onHandlerFinishedClear();
    MycatMonitor.onPacketExchangerClear(mycatSession);
  }

  public void proxyBackend(MycatSession mycat, byte[] payload, String dataNodeName,
      MySQLDataSourceQuery query, ResponseType responseType) {
    proxyBackend(mycat, payload, dataNodeName, query, responseType,
        DEFAULT_BACKEND_SESSION_REQUEST_FAILED_CALLBACK);

  }

  public void proxyBackend(MycatSession mycat, byte[] payload, String dataNodeName,
      MySQLDataSourceQuery query, ResponseType responseType, PacketExchangerCallback finallyCallBack) {
    byte[] bytes = MySQLPacketUtil.generateMySQLPacket(0, payload);
    MySQLProxyNIOHandler
        .INSTANCE.proxyBackend(mycat, bytes, dataNodeName, query, responseType,
        MySQLProxyNIOHandler.INSTANCE, finallyCallBack
    );
  }

  public void proxyBackendWithRawPacket(MycatSession mycat, byte[] packet, String dataNodeName,
      MySQLDataSourceQuery query, ResponseType responseType) {
    MySQLProxyNIOHandler
        .INSTANCE.proxyBackend(mycat, packet, dataNodeName, query, responseType,
        MySQLProxyNIOHandler.INSTANCE, DEFAULT_BACKEND_SESSION_REQUEST_FAILED_CALLBACK
    );
  }

  public void proxyWithCollectorCallback(MycatSession mycat, byte[] payload, String dataNodeName,
      MySQLDataSourceQuery query, ResponseType responseType, MySQLPacketCallback callback) {
    proxyWithCollectorCallback(mycat, payload, dataNodeName, query, responseType, callback,
        DEFAULT_BACKEND_SESSION_REQUEST_FAILED_CALLBACK);
  }

  public void proxyWithCollectorCallback(MycatSession mycat, byte[] payload, String dataNodeName,
      MySQLDataSourceQuery query, ResponseType responseType, MySQLPacketCallback callback,
      PacketExchangerCallback finallyCallBack) {
    byte[] bytes = MySQLPacketUtil.generateMySQLPacket(0, payload);
    MySQLProxyNIOHandler
        .INSTANCE.proxyBackend(mycat, bytes, dataNodeName, query, responseType,
        new MySQLCollectorExchanger(callback), finallyCallBack
    );
  }

  private void onBackendResponse(MySQLClientSession mysql) throws IOException {
    MycatSession mycatSession = mysql.getMycatSession();
    if (!mysql.readFromChannel()) {
      return;
    }
    mysql.setRequestSuccess(true);
    MycatMonitor.onPacketExchangerRead(mysql);
    ProxyBuffer proxyBuffer = mysql.currentProxyBuffer();
    MySQLPacket mySQLPacket = (MySQLPacket) proxyBuffer;
    MySQLPacketResolver packetResolver = mysql.getBackendPacketResolver();
    int startIndex = mySQLPacket.packetReadStartIndex();
    int endPos = startIndex;
    while (mysql.readPartProxyPayload()) {
      endPos = packetResolver.getEndPos();
      mySQLPacket.packetReadStartIndex(endPos);
    }
    proxyBuffer.channelWriteStartIndex(startIndex);
    proxyBuffer.channelWriteEndIndex(endPos);

    mycatSession.writeToChannel();
    return;
  }

  private boolean onBackendWriteFinished(MySQLClientSession mysql) {
    ResponseType responseType = mysql.getResponseType();
    if (responseType == ResponseType.NO_RESPONSE) {

      return true;
    } else {
      ProxyBuffer proxyBuffer = mysql.currentProxyBuffer();
      proxyBuffer.channelReadStartIndex(0);
      proxyBuffer.channelReadEndIndex(0);
      mysql.change2ReadOpts();
    }
    switch (mysql.getResponseType()) {
      case NO_RESPONSE: {
        throw new MycatException("unknown state");
      }
      case MULTI_RESULTSET: {
        mysql.prepareReveiceMultiResultSetResponse();
        break;
      }
      case PREPARE_OK:{
        mysql.prepareReveicePrepareOkResponse();
        break;
      }
      case QUERY: {
        mysql.prepareReveiceResponse();
        break;
      }
    }
    return false;
  }

  private boolean onFrontWriteFinished(MycatSession mycat) {
    MySQLClientSession mysql = mycat.getMySQLSession();
    ProxyBuffer proxyBuffer = mycat.currentProxyBuffer();
    if (proxyBuffer.channelWriteFinished() && mysql.isResponseFinished()) {
      mycat.change2ReadOpts();
      return true;
    } else {
      mysql.change2ReadOpts();
      int writeEndIndex = proxyBuffer.channelWriteEndIndex();
      proxyBuffer.channelReadStartIndex(writeEndIndex);
      return false;
    }
  }

  public static class MySQLProxyNIOHandler implements BackendNIOHandler<MySQLClientSession> {

    public static final MySQLProxyNIOHandler INSTANCE = new MySQLProxyNIOHandler();
    protected final static MycatLogger LOGGER = MycatLoggerFactory
        .getLogger(MySQLProxyNIOHandler.class);
    static final MySQLPacketExchanger HANDLER = MySQLPacketExchanger.INSTANCE;


    public void proxyBackend(MycatSession mycat, byte[] packetData, String dataNodeName,
        MySQLDataSourceQuery query, ResponseType responseType, MySQLProxyNIOHandler proxyNIOHandler,
        PacketExchangerCallback finallyCallBack) {
      MySQLTaskUtil.withBackend(mycat, dataNodeName, query, new SessionSyncCallback() {
        @Override
        public void onSession(MySQLClientSession mysql, Object sender, Object attr) {
          proxyNIOHandler.proxyBackend(mysql, finallyCallBack, responseType, mycat, packetData);
        }

        @Override
        public void onException(Exception exception, Object sender, Object attr) {
          MycatMonitor.onGettingBackendException(mycat, dataNodeName, exception);
          finallyCallBack.onRequestMySQLException(mycat, exception, attr);
        }

        @Override
        public void onErrorPacket(ErrorPacketImpl errorPacket, boolean monopolize,
            MySQLClientSession mysql, Object sender, Object attr) {
          finallyCallBack.onRequestMySQLException(mycat,
              new MycatException(errorPacket.getErrorMessageString()), attr);
        }
      });
    }

    public void proxyBackend(MySQLClientSession mysql, PacketExchangerCallback finallyCallBack,
        ResponseType responseType, MycatSession mycat, byte[] packetData) {
      try {
        mysql.setCallBack(finallyCallBack);
        Objects.requireNonNull(responseType);

        mysql.setResponseType(responseType);
        switch (responseType) {
          case NO_RESPONSE:
            break;
          case MULTI_RESULTSET:
            mysql.setResponseType(responseType);
            mysql.prepareReveiceMultiResultSetResponse();
            break;
          case PREPARE_OK:
            mysql.prepareReveicePrepareOkResponse();
            break;
          case QUERY:
            mysql.prepareReveiceResponse();
            break;
        }
        mysql.switchNioHandler(INSTANCE);
        mycat.setMySQLSession(mysql);
        mycat.switchWriteHandler(WriteHandler.INSTANCE);
        mycat.currentProxyBuffer().newBuffer(packetData);
        mycat.setMySQLSession(mysql);
        mysql.setMycatSession(mycat);
        mysql.writeProxyBufferToChannel(mycat.currentProxyBuffer());
        MycatMonitor.onBindMySQLSession(mycat, mysql);
      } catch (Exception e) {
        onExceptionClearCloseInRequest(mycat, e, finallyCallBack);
        return;
      }
    }

    /**
     * @param mysql change this function also change the prepare statement
     */
    @Override
    public void onSocketRead(MySQLClientSession mysql) {
      try {
        HANDLER.onBackendResponse(mysql);
      } catch (Exception e) {
        MycatSession mycat = mysql.getMycatSession();
        if (mysql.isRequestSuccess()) {
          onExceptionClearCloseInResponse(mycat, e);
          return;
        } else {
          onExceptionClearCloseInRequest(mycat, e, mysql.getCallBack());
          return;
        }
      }
    }

    @Override
    public void onSocketWrite(MySQLClientSession session) {
      try {
        session.writeToChannel();
        MycatMonitor.onPacketExchangerWrite(session);
      } catch (Exception e) {
        onExceptionClearCloseInResponse(session.getMycatSeesion(), e);
      }
    }

    @Override
    public void onWriteFinished(MySQLClientSession session) {
      boolean b = HANDLER.onBackendWriteFinished(session);
      session.setRequestSuccess(false);
      if (b) {
        MycatSession mycatSession = session.getMycatSession();
        onClearInNormalResponse(mycatSession, session);
      }
    }

    @Override
    public void onException(MySQLClientSession session, Exception e) {
      MycatSession mycatSeesion = session.getMycatSeesion();
      onExceptionClearCloseInResponse(mycatSeesion, e);
    }
  }

  private static class MySQLCollectorExchanger extends MySQLProxyNIOHandler {

    final MySQLPacketCallback callback;

    public MySQLCollectorExchanger(MySQLPacketCallback resultSetCollector) {
      this.callback = resultSetCollector;
    }

    @Override
    public void onSocketRead(MySQLClientSession mysql) {
      try {
        onBackendResponse(mysql);
      } catch (Exception e) {
        MycatSession mycat = mysql.getMycatSession();
        if (mysql.isRequestSuccess()) {
          onExceptionClearCloseInResponse(mycat, e);
          return;
        } else {
          onExceptionClearCloseInRequest(mycat, e, mysql.getCallBack());
          return;
        }
      }
    }

    private void onBackendResponse(MySQLClientSession mysql) throws IOException {
      MycatSession mycatSession = mysql.getMycatSession();
      if (!mysql.readFromChannel()) {
        return;
      }
      mysql.setRequestSuccess(true);
      MycatMonitor.onPacketExchangerRead(mysql);
      ProxyBuffer proxyBuffer = mysql.currentProxyBuffer();
      MySQLPacket mySQLPacket = (MySQLPacket) proxyBuffer;
      MySQLPacketResolver packetResolver = mysql.getBackendPacketResolver();
      int startIndex = mySQLPacket.packetReadStartIndex();
      int endPos = startIndex;
      while (mysql.readPartProxyPayload()) {
        MySQLPayloadType payloadType = mysql.getPayloadType();
        int sIndex = mySQLPacket.packetReadStartIndex();
        int eIndex = mySQLPacket.packetReadEndIndex();
        switch (payloadType) {
          case REQUEST:
            callback.onRequest(mySQLPacket, sIndex, eIndex);
            break;
          case LOAD_DATA_REQUEST:
            callback.onLoadDataRequest(mySQLPacket, sIndex, eIndex);
            break;
          case REQUEST_COM_QUERY:
            callback.onRequestComQuery(mySQLPacket, sIndex, eIndex);
            break;
          case REQUEST_SEND_LONG_DATA:
            callback.onLoadDataRequest(mySQLPacket, sIndex, eIndex);
            break;
          case REQUEST_PREPARE:
            callback.onReqeustPrepareStatement(mySQLPacket, sIndex, eIndex);
            break;
          case REQUEST_COM_STMT_CLOSE:
            callback.onRequestComStmtClose(mySQLPacket, sIndex, eIndex);
            break;
          case FIRST_ERROR: {
            ErrorPacketImpl packet = new ErrorPacketImpl();
            packet.readPayload(mySQLPacket);
            callback.onFirstError(packet);
            break;
          }
          case FIRST_OK:
            callback.onOk(mySQLPacket, sIndex, endPos);
            break;
          case FIRST_EOF:
            callback.onEof(mySQLPacket, sIndex, eIndex);
            break;
          case COLUMN_COUNT:
            callback.onColumnCount(packetResolver.getColumnCount());
            break;
          case COLUMN_DEF:
            callback.onColumnDef(mySQLPacket, sIndex, eIndex);
            break;
          case COLUMN_EOF:
            callback.onColumnDefEof(mySQLPacket, sIndex, eIndex);
            break;
          case TEXT_ROW:
            callback.onTextRow(mySQLPacket, sIndex, eIndex);
            break;
          case BINARY_ROW:
            callback.onBinaryRow(mySQLPacket, sIndex, eIndex);
            break;
          case ROW_EOF:
            callback.onRowEof(mySQLPacket, sIndex, eIndex);
            break;
//          case ROW_FINISHED:
//            break;
          case ROW_OK: {
            callback.onRowOk(mySQLPacket, sIndex, eIndex);
            break;
          }
          case ROW_ERROR: {
            ErrorPacketImpl packet = new ErrorPacketImpl();
            packet.readPayload(mySQLPacket);
            callback.onRowError(packet, sIndex, eIndex);
            break;
          }
          case PREPARE_OK:
            callback.onPrepareOk(packetResolver);
            break;
          case PREPARE_OK_PARAMER_DEF:
            callback.onPrepareOkParameterDef(mySQLPacket, sIndex, eIndex);
            break;
          case PREPARE_OK_COLUMN_DEF:
            callback.onPrepareOkParameterDef(mySQLPacket, sIndex, eIndex);
            break;
          case PREPARE_OK_COLUMN_DEF_EOF:
            callback.onPrepareOkParameterDef(mySQLPacket, sIndex, eIndex);
            break;
          case PREPARE_OK_PARAMER_DEF_EOF:
            callback.onPrepareOkParameterDef(mySQLPacket, sIndex, eIndex);
            break;
        }
        endPos = packetResolver.getEndPos();
        mySQLPacket.packetReadStartIndex(endPos);
      }
      proxyBuffer.channelWriteStartIndex(startIndex);
      proxyBuffer.channelWriteEndIndex(endPos);

      if (packetResolver.isResponseFinished()) {
        callback.onFinishedCollect(mysql);
      }

      mycatSession.writeToChannel();
      return;
    }

    @Override
    public void onException(MySQLClientSession session, Exception e) {
      super.onException(session, e);
      callback.onFinishedCollectException(session, e);
    }
  }

  /**
   * 代理模式前端写入处理器
   */
  private enum WriteHandler implements MycatSessionWriteHandler {
    INSTANCE;

    @Override
    public void writeToChannel(MycatSession mycat) throws IOException {
      try {
        mycat.getMySQLSession().clearReadWriteOpts();
        ProxyBuffer proxyBuffer = mycat.currentProxyBuffer();
        int oldIndex = proxyBuffer.channelWriteStartIndex();
        int endIndex = proxyBuffer.channelWriteEndIndex();
        MycatMonitor.onPacketExchangerWrite(mycat);
        proxyBuffer.writeToChannel(mycat.channel());

        MycatMonitor.onFrontWrite(mycat, proxyBuffer.currentByteBuffer(), oldIndex,
            endIndex - oldIndex);
        mycat.updateLastActiveTime();

        if (!proxyBuffer.channelWriteFinished()) {
          mycat.change2WriteOpts();
        } else {
          MySQLClientSession mysql = mycat.getMySQLSession();
          if (mysql == null) {
            assert false;
          } else {
            boolean b = MySQLPacketExchanger.INSTANCE.onFrontWriteFinished(mycat);
            if (b) {
              MySQLPayloadType payloadType = mysql.getPayloadType();
              if (payloadType == MySQLPayloadType.LOAD_DATA_REQUEST) {
                if (!mycat.shouldHandleContentOfFilename()){
                  mysql.setMonopolizeType(MySQLSessionMonopolizeType.LOAD_DATA);
                  mycat.setHandleContentOfFilename(true);
                }
              }
              mycat.getIOThread().getSelector().wakeup();
              onClearInNormalResponse(mycat, mysql);
            }
          }
        }
      } catch (Exception e) {
        onExceptionClearCloseInResponse(mycat, e);
      }
    }

    @Override
    public void onException(MycatSession session, Exception e) {
      onExceptionClearCloseInResponse(session, e);
    }
  }

  public interface PacketExchangerCallback extends TaskCallBack<PacketExchangerCallback> {

    void onRequestMySQLException(MycatSession mycat, Exception e, Object attr);
  }
}
