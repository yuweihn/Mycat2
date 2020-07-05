/**
 * Copyright (C) <2020>  <chen junwen>
 * <p>
 * This program is open software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat;

import io.mycat.beans.mycat.JdbcRowBaseIterator;
import io.mycat.beans.resultset.MycatResponse;
import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.beans.resultset.MycatUpdateResponse;
import io.mycat.proxy.session.MycatSession;
import io.mycat.resultset.TextResultSetResponse;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class SQLExecuterWriter {

    @SneakyThrows
    public static void executeQuery(MycatSession session, Connection connection, String sql) {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        JdbcRowBaseIterator jdbcRowBaseIterator = new JdbcRowBaseIterator(null, statement, statement.executeQuery(sql), resultSet, sql);
        writeToMycatSession(session, new MycatResponse[]{new TextResultSetResponse(jdbcRowBaseIterator)});
    }

    @SneakyThrows
    public static MycatResponse[] executeQuery(Connection connection, String sql) {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        return getMycatResponses(statement, resultSet);
    }

    @NotNull
    public static MycatResponse[] getMycatResponses(Statement statement, ResultSet resultSet) {
        JdbcRowBaseIterator jdbcRowBaseIterator = new JdbcRowBaseIterator(null, statement,resultSet, null,null);
        return new MycatResponse[]{new TextResultSetResponse(jdbcRowBaseIterator)};
    }
    public static void writeToMycatSession(MycatSession session, final MycatResponse... sqlExecuters) {
        writeToMycatSession(session, Arrays.asList(sqlExecuters));
    }
    public static void writeToMycatSession(MycatSession session, final List<MycatResponse> sqlExecuters) {
        if (sqlExecuters.size() == 0) {
            session.writeOkEndPacket();
            return;
        }
        final MycatResponse endSqlExecuter = sqlExecuters.get(sqlExecuters.size() - 1);
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
                            long updateCount = currentUpdateResponse.getUpdateCount();
                            long lastInsertId1 = currentUpdateResponse.getLastInsertId();
                            session.setAffectedRows(updateCount);
                            session.setLastInsertId(lastInsertId1);
                            session.writeOk(endSqlExecuter != sqlExecuter);
                            break;
                        }
                        case ERROR:
                            throw new UnsupportedOperationException();
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
                        case BINARY_RRESULTSET:
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
                }
            }
            return;
        } catch (Exception e) {
            session.setLastMessage(e);
            session.writeErrorEndPacketBySyncInProcessError();
        }
        return;
    }
}