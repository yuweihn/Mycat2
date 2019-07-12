package io.mycat.datasource.jdbc;


import io.mycat.MycatException;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;

/**
 * @author jamie12221 date 2019-05-10 14:46 该类型需要并发处理
 **/
public class JdbcDataSourceManager implements SessionManager {

  final static MycatLogger LOGGER = MycatLoggerFactory.getLogger(JdbcDataSourceManager.class);
  private final static Set<String> AVAILABLE_JDBC_DATA_SOURCE = new HashSet<>();
  private final SessionProvider sessionProvider;
  private final ConcurrentHashMap<Integer, JdbcSession> allSessions = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<JdbcDataSource, DataSource> dataSourceMap = new ConcurrentHashMap<>();
  private DatasourceProvider datasourceProvider;

  static {
    // 加载可能的驱动
    List<String> drivers = Arrays.asList(
        "com.mysql.jdbc.Driver");
    for (String driver : drivers) {
      try {
        Class.forName(driver);
        AVAILABLE_JDBC_DATA_SOURCE.add(driver);
      } catch (ClassNotFoundException ignored) {
      }
    }
  }

  public JdbcDataSourceManager(
      SessionProvider sessionProvider,
      DatasourceProvider provider) {
    this.sessionProvider = sessionProvider;
    this.datasourceProvider = provider;
  }

  public List<JdbcSession> getAllSessions() {
    return new ArrayList<>(allSessions.values());
  }

  @Override
  public int currentSessionCount() {
    return allSessions.size();
  }


  private DataSource getPool(JdbcDataSource datasource) {
    return dataSourceMap.compute(datasource,
        (jdbcDataSource, dataSource) -> {
          if (dataSource == null) {
            dataSource = datasourceProvider
                .createDataSource(datasource.getUrl(), datasource.getUsername(),
                    datasource.getPassword());
          }
          return dataSource;
        });
  }

  @Override
  public void clearAndDestroyDataSource(boolean normal, JdbcDataSource key, String reason) {
    for (Entry<Integer, JdbcSession> entry : allSessions.entrySet()) {
      JdbcSession session = entry.getValue();
      if (session.getDatasource().equals(key)) {
        session.close(normal, reason);
      }
    }
  }


  public JdbcSession createSession(JdbcDataSource key) throws MycatException {
    Connection connection = null;
    try {
      connection = getConnection(key);
    } catch (SQLException e) {
      throw new MycatException(e);
    }
    int sessionId = sessionProvider.sessionId();
    JdbcSession jdbcSession = new JdbcSession(sessionId, key);
    jdbcSession.wrap(connection);
    allSessions.put(sessionId, jdbcSession);
    return jdbcSession;
  }

  @Override
  public void closeSession(JdbcSession session, boolean normal, String reason) {
    try {
      session.connection.close();
    } catch (Exception e) {
      LOGGER.error("{}", e);
    }
    allSessions.remove(session.sessionId());
  }


  public Connection getConnection(JdbcDataSource key) throws SQLException {
    DataSource pool = getPool(key);
    return pool.getConnection();
  }

  interface SessionProvider {

    int sessionId();
  }

  interface DatasourceProvider {

    DataSource createDataSource(String url, String username, String password);
  }
}
