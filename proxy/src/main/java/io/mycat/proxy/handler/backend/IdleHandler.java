package io.mycat.proxy.handler.backend;

import static io.mycat.logTip.SessionTip.UNKNOWN_IDLE_RESPONSE;

import io.mycat.MycatExpection;
import io.mycat.proxy.handler.NIOHandler;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.session.MySQLClientSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum IdleHandler implements NIOHandler<MySQLClientSession> {
  INSTANCE;
  protected final static Logger logger = LoggerFactory.getLogger(
      IdleHandler.class);

  @Override
  public void onSocketRead(MySQLClientSession session) {
    String message = UNKNOWN_IDLE_RESPONSE.getMessage();
    MycatMonitor.onIdleReadException(session,new MycatExpection(message));
    session.close(false,message);
  }

  @Override
  public void onSocketWrite(MySQLClientSession session) {

  }

  @Override
  public void onWriteFinished(MySQLClientSession session) {
    assert false;
  }

  @Override
  public void onException(MySQLClientSession session, Exception e) {
    logger.error("{}", e);
    session.close(false, e);
  }

}