package io.mycat.replica.heartbeat;

import io.mycat.beans.mysql.MySQLCommandType;
import io.mycat.beans.mysql.packet.ErrorPacketImpl;
import io.mycat.mysqlapi.collector.CollectorUtil;
import io.mycat.mysqlapi.collector.CommonSQLCallback;
import io.mycat.mysqlapi.collector.OneResultSetCollector;
import io.mycat.mysqlapi.collector.TextResultSetTransforCollector;
import io.mycat.proxy.callback.ResultSetCallBack;
import io.mycat.proxy.callback.SessionCallBack;
import io.mycat.proxy.handler.backend.TextResultSetHandler;
import io.mycat.proxy.session.MySQLClientSession;

/**
 * @author : zhangwy
 * @author : chenjunwen
 * @version V1.1 date Date : 2019年07月19日 17:46
 */
public class HeartBeatAsyncTaskCallBack implements SessionCallBack<MySQLClientSession> {

  protected volatile boolean isQuit = false;
  protected final HeartbeatDetector heartbeatDetector;
  private CommonSQLCallback commonSQLCallback;

  public HeartBeatAsyncTaskCallBack(HeartbeatDetector heartbeatDetector,
      CommonSQLCallback commonSQLCallback) {
    this.heartbeatDetector = heartbeatDetector;
    this.commonSQLCallback = commonSQLCallback;
  }

   void setQuit(boolean quit) {
    isQuit = quit;
  }

  private void onStatus(int errorStatus) {
    heartbeatDetector.getHeartbeatManager()
        .setStatus(errorStatus);
  }


  @Override
  public void onSession(MySQLClientSession session, Object sender, Object attr) {
    if (isQuit == false) {
      OneResultSetCollector collector = new OneResultSetCollector();
      TextResultSetTransforCollector transfor = new TextResultSetTransforCollector(
          collector);
      TextResultSetHandler queryResultSetTask = new TextResultSetHandler(transfor, (i) -> true);
      queryResultSetTask.request(session, MySQLCommandType.COM_QUERY, commonSQLCallback.getSql(),
          new ResultSetCallBack<MySQLClientSession>() {
            @Override
            public void onFinishedSendException(Exception exception, Object sender,
                Object attr) {
              if (isQuit == false) {
                commonSQLCallback.onException(exception);
                onStatus(DatasourceStatus.ERROR_STATUS);
              }
            }

            @Override
            public void onFinishedException(Exception exception, Object sender, Object attr) {
              if (isQuit == false) {
                commonSQLCallback.onException(exception);
                onStatus(DatasourceStatus.ERROR_STATUS);
              }

            }

            @Override
            public void onFinished(boolean monopolize, MySQLClientSession mysql,
                Object sender, Object attr) {
              try {
                if (isQuit == false) {
                  commonSQLCallback.process(CollectorUtil.toList(collector));
                }
              } finally {
                mysql.getSessionManager().addIdleSession(mysql);
              }
            }

            @Override
            public void onErrorPacket(ErrorPacketImpl errorPacket, boolean monopolize,
                MySQLClientSession mysql, Object sender, Object attr) {
              if (isQuit == false) {
                commonSQLCallback.onError(errorPacket.getErrorMessageString());
                onStatus(DatasourceStatus.ERROR_STATUS);
              }
            }
          })
      ;
    }
  }

  @Override
  public void onException(Exception e, Object sender, Object attr) {
    if (isQuit == false) {
      onStatus(DatasourceStatus.ERROR_STATUS);
    }
  }
}
