package io.mycat.proxy.handler.backend;

import io.mycat.MycatException;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.buffer.ProxyBufferImpl;
import io.mycat.proxy.callback.RequestCallback;
import io.mycat.proxy.handler.NIOHandler;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.reactor.MycatReactorThread;
import io.mycat.proxy.session.MySQLClientSession;

/**
 * @author jamie12221
 *  date 2019-05-22 11:13
 **/
public enum RequestHandler implements NIOHandler<MySQLClientSession> {
  INSTANCE;
  protected final static MycatLogger LOGGER = MycatLoggerFactory
      .getLogger(BackendConCreateHandler.class);
  public void request(MySQLClientSession session, byte[] packet, RequestCallback callback) {
    session.setCallBack(callback);
    try {
      session.setCallBack(callback);
      session.switchNioHandler(this);
      MycatReactorThread thread = (MycatReactorThread)Thread.currentThread();
      session.setCurrentProxyBuffer(new ProxyBufferImpl(thread.getBufPool()));
      session.writeProxyBufferToChannel(packet);
    } catch (Exception e) {
      MycatMonitor.onRequestException(session,e);
      onException(session, e);
      callback.onFinishedSendException(e, this, null);
    }
  }

  @Override
  public void onSocketRead(MySQLClientSession session) {
    Exception e = new MycatException("unknown read data");
    try {
      session.readFromChannel();
    } catch (Exception e1) {
      e = e1;
    }
    MycatMonitor.onRequestException(session,e);
    RequestCallback callback = session.getCallBack();
    onException(session, e);
    callback.onFinishedSendException(e, this, null);
  }

  @Override
  public void onSocketWrite(MySQLClientSession session) {
    try {
      session.writeToChannel();
    } catch (Exception e) {
      MycatMonitor.onRequestException(session,e);
      RequestCallback callback = session.getCallBack();
      onException(session, e);
      callback.onFinishedSendException(e, this, null);
    }
  }

  @Override
  public void onWriteFinished(MySQLClientSession session) {
    RequestCallback callback = session.getCallBack();
    onClear(session);
    callback.onFinishedSend(session, this, null);
  }

  @Override
  public void onException(MySQLClientSession session, Exception e) {
    MycatMonitor.onRequestException(session,e);
    LOGGER.error("{}", e);
    onClear(session);
    session.setCallBack(null);
    session.close(false, e);
  }

  public void onClear(MySQLClientSession session) {
    session.resetPacket();
    session.setCallBack(null);
    session.switchNioHandler(null);
    MycatMonitor.onRequestClear(session);
  }
}
