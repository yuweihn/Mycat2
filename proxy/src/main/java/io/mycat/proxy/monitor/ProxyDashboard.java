package io.mycat.proxy.monitor;

import io.mycat.beans.MySQLSessionMonopolizeType;
import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.buffer.BufferPool;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.buffer.ProxyBuffer;
import io.mycat.proxy.reactor.MycatReactorThread;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MySQLSessionManager;
import io.mycat.proxy.session.MycatSession;
import io.mycat.proxy.session.SessionManager.FrontSessionManager;
import io.mycat.replica.MySQLDatasource;
import io.mycat.security.MycatUser;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum ProxyDashboard {
  INSTANCE;
  protected final static Logger LOGGER = LoggerFactory.getLogger("resourceLogger");

  public void collectInfo(ProxyRuntime runtime) {
    LOGGER.info("---------------------------dashboard---------------------------");
    for (MycatReactorThread thread : runtime.getMycatReactorThreads()) {
      BufferPool bufPool = thread.getBufPool();
      Map<Long, Long> memoryUsage = bufPool.getNetDirectMemoryUsage();
      for (Entry<Long, Long> entry : memoryUsage.entrySet()) {
        LOGGER.info("threadId:{}  buffer size:{}", entry.getKey(), entry.getValue());
      }
      FrontSessionManager<MycatSession> frontManager = thread.getFrontManager();
      for (MycatSession mycat : frontManager.getAllSessions()) {
        MycatUser user = mycat.getUser();
        LOGGER.info("---------------------------mycat---------------------------");
        LOGGER.info("mycat id:{}  username:{}", mycat.sessionId(), user.getUserName());
        ProxyBuffer proxyBuffer = mycat.currentProxyBuffer();
        LinkedList<ByteBuffer> writeQueue = mycat.writeQueue();
        LOGGER.info("byteBuffer:{} in proxyBuffer,writeQueue size:{}",
            proxyBuffer.currentByteBuffer(), writeQueue);
        boolean open = mycat.isOpen();
        LOGGER.info("open :{} isClosed", open, mycat.isClosed());
        String schema = mycat.getSchema();
        Charset charsetObject = mycat.charset();
        String characterSetResults = mycat.getCharacterSetResults();
        String charsetName = mycat.getCharsetName();
        LOGGER.info("charsetName:{} charsetObject :{} characterSetResults:{} ", charsetName,
            charsetObject, characterSetResults);
        MySQLAutoCommit autoCommit = mycat.getAutoCommit();
        MySQLIsolation isolation = mycat.getIsolation();
        LOGGER.info("autoCommit :{} isolation :{}", autoCommit, isolation);
        int lastErrorCode = mycat.getLastErrorCode();
        String lastMessage = mycat.getLastMessage();
        LOGGER.info("lastErrorCode :{} lastMessage:{}", lastErrorCode, lastMessage);
        if (schema != null) {
          LOGGER.info("schema :{}", schema);
        }
        String dataNode = mycat.getDataNode();
        LOGGER.info("dataNode :{}", dataNode);
        MySQLClientSession mySQLSession = mycat.getMySQLSession();
        if (mySQLSession != null) {
          LOGGER.info("backendId:{}", mySQLSession.sessionId());
        }
      }

      MySQLSessionManager manager = thread.getMySQLSessionManager();
      for (MySQLClientSession mysql : manager.getAllSessions()) {
        LOGGER.info("---------------------------mysql---------------------------");
        MySQLAutoCommit automCommit = mysql.isAutomCommit();
        MySQLIsolation isolation = mysql.getIsolation();
        LOGGER.info("automCommit:{} isolation:{}", automCommit, isolation);
        String charset = mysql.getCharset();
        String characterSetResult = mysql.getCharacterSetResult();
        LOGGER.info("charset:{} characterSetResult:{}", charset, characterSetResult);

        String lastMessage = mysql.getLastMessage();
        LOGGER.info("lastMessage:{}", lastMessage);
        MycatSession mycatSeesion = mysql.getMycatSeesion();
        if (mycatSeesion != null) {
          LOGGER.info("bind mycat session:{}", mycatSeesion.sessionId());
        }
        LOGGER.info("open:{} isClose:{}", mysql.isOpen(), mysql.isClosed());
        ProxyBuffer proxyBuffer = mysql.currentProxyBuffer();
        LOGGER.info("proxyBuffer:{}", proxyBuffer);
        if (proxyBuffer != null) {
          ByteBuffer byteBuffer = proxyBuffer.currentByteBuffer();
          LOGGER.info("byteBuffer:{}", byteBuffer);
        }
        boolean idle = mysql.isIdle();
        MySQLSessionMonopolizeType type = mysql.getMonopolizeType();
        LOGGER.info("idle:{},monopolizeType:{}", idle, type);
      }
    }
    Collection<MySQLDatasource> datasourceList = runtime.getMySQLDatasourceList();
    LOGGER.info("---------------------------datasource---------------------------");
    for (MySQLDatasource datasource : datasourceList) {
      String name = datasource.getName();
      int sessionCounter = datasource.getSessionCounter();
      LOGGER.info("dataSourceName:{} sessionCounter", name, sessionCounter);
    }
  }
}