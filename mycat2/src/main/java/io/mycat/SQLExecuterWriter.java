package io.mycat;

import io.mycat.beans.resultset.MycatResponse;
import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.beans.resultset.MycatUpdateResponse;
import io.mycat.datasource.jdbc.resultset.JdbcRowBaseIteratorImpl;
import io.mycat.datasource.jdbc.resultset.TextResultSetResponse;
import io.mycat.proxy.session.MycatSession;
import lombok.SneakyThrows;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Iterator;

public class SQLExecuterWriter {

    public static void executeQuery(MycatSession session, Connection connection, String sql) {
        MycatResponse[] mycatResponses = executeQuery(connection, sql);
        writeToMycatSession(session, mycatResponses);
    }

    @SneakyThrows
    public static MycatResponse[] executeQuery(Connection connection, String sql) {
        Statement statement = null;
        statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        JdbcRowBaseIteratorImpl jdbcRowBaseIterator = new JdbcRowBaseIteratorImpl(statement, resultSet);
        return new MycatResponse[]{new TextResultSetResponse(jdbcRowBaseIterator)};
    }

    public static void writeToMycatSession(MycatSession session, final MycatResponse... sqlExecuters) {
        if (sqlExecuters.length == 0) {
            session.writeOkEndPacket();
            return;
        }
        final MycatResponse endSqlExecuter = sqlExecuters[sqlExecuters.length - 1];
        try {
            for (MycatResponse sqlExecuter : sqlExecuters) {
                try (MycatResponse resultSet = sqlExecuter) {
                    switch (resultSet.getType()) {
                        case RRESULTSET: {
                            MycatResultSetResponse currentResultSet = (MycatResultSetResponse) resultSet;
                            session.writeColumnCount(currentResultSet.columnCount());
                            Iterator<byte[]> columnDefPayloadsIterator = currentResultSet
                                    .columnDefIterator();
                            while (columnDefPayloadsIterator.hasNext()) {
                                session.writeBytes(columnDefPayloadsIterator.next(), false);
                            }
                            session.writeColumnEndPacket();
                            Iterator<byte[]> rowIterator = currentResultSet.rowIterator();
                            while (rowIterator.hasNext()) {
                                session.writeBytes(rowIterator.next(), false);
                            }
                            session.writeRowEndPacket(endSqlExecuter != sqlExecuter, false);
                            break;
                        }
                        case UPDATEOK: {
                            MycatUpdateResponse currentUpdateResponse = (MycatUpdateResponse) resultSet;
                            int updateCount = currentUpdateResponse.getUpdateCount();
                            long lastInsertId1 = currentUpdateResponse.getLastInsertId();
                            session.setWarningCount(updateCount);
                            session.setLastInsertId(lastInsertId1);
                            session.writeOk(endSqlExecuter != sqlExecuter);
                            break;
                        }
                        case ERROR:
                            break;
                        case RRESULTSET_BYTEBUFFER: {
                            MycatResultSetResponse currentResultSet = (MycatResultSetResponse) resultSet;
                            session.writeColumnCount(currentResultSet.columnCount());
                            Iterator<ByteBuffer> columnDefPayloadsIterator = currentResultSet
                                    .columnDefIterator();
                            while (columnDefPayloadsIterator.hasNext()) {
                                session.writeBytes(columnDefPayloadsIterator.next(), false);
                            }
                            session.writeColumnEndPacket();
                            Iterator<ByteBuffer> rowIterator = currentResultSet.rowIterator();
                            while (rowIterator.hasNext()) {
                                session.writeBytes(rowIterator.next(), false);
                            }
                            session.writeRowEndPacket(endSqlExecuter != sqlExecuter, false);
                            break;
                        }
                    }
                }
            }
            return;
        } catch (Exception e) {
            session.setLastMessage(e);
        }
        session.writeErrorEndPacket();
        return;
    }
}