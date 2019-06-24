package io.mycat.jdbc;

import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLIsolation;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author jamie12221
 *  date 2019-05-10 14:51
 **/
public class JdbcSession {

  private final Connection connection;
  private final JdbcDataSource key;

  public JdbcSession(Connection connection, JdbcDataSource key) {

    this.connection = connection;
    this.key = key;
  }

  public JdbcDataSource getDatasource() {
    return key;
  }

  public void sync(String schema, MySQLIsolation isolation,
      MySQLAutoCommit autoCommit,String charset) throws SQLException {
    switch (isolation) {
      case READ_UNCOMMITTED:
        connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        break;
      case READ_COMMITTED:
        connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        break;
      case REPEATED_READ:
        connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        break;
      case SERIALIZABLE:
        connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        break;
    }
    connection.setSchema(schema);
    connection.setAutoCommit(autoCommit == MySQLAutoCommit.ON);
    connection.setClientInfo("characterEncoding",charset);

  }

  public boolean query(String s) throws SQLException {
    boolean success = true;
    try(Statement statement = connection.createStatement()){
      ResultSet resultSet = statement.executeQuery(s);
      while (resultSet.next()) {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
          String catalogName = metaData.getColumnName(9);
          long bytes = resultSet.getLong(9);
          boolean b = resultSet.wasNull();
          System.out.println(catalogName);
          System.out.println(bytes);
        }
      }
    }
    return success;
  }
}
