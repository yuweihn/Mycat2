package io.mycat.mycat2.tasks;

import io.mycat.mycat2.AbstractMySQLSession;
import io.mycat.mycat2.beans.MycatException;
import io.mycat.mysql.ComQueryState;
import io.mycat.mysql.MySQLPacketInf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * task处理结果集的模板类
 * <p>
 * Created by ynfeng on 2017/8/28.
 */
public abstract class BackendIOTaskWithResultSet<T extends AbstractMySQLSession> extends AbstractBackendIOTask<T> {
    private static Logger logger = LoggerFactory.getLogger(BackendIOTaskWithResultSet.class);
    ResultSetState curRSState;


    public BackendIOTaskWithResultSet(T session, boolean useNewBuffer) {
        super(session, useNewBuffer);
        this.curRSState = ResultSetState.RS_STATUS_NORMAL;
    }

    @Override
    public void onSocketRead(T session) throws IOException {
        try {
            if (!session.readFromChannel()) {
                return;
            }
            MySQLPacketInf curPacketInf = session.curPacketInf;
            while (curPacketInf.readFully()) {
                switch (curPacketInf.mysqlPacketType) {
                    case ERROR:
                        onRsFinish(session, false, "错误包");
                        break;
                    case OK:
                    case EOF:
                        if (curPacketInf.state == ComQueryState.COMMAND_END) {
                            onRsFinish(session, true, null);
                        }
                        break;
                    case COLUMN_COUNT:
                        onRsColCount(session);
                        break;
                    case COLUMN_DEFINITION:
                        onRsColDef(session);
                        break;
                    case TEXT_RESULTSET_ROW:
                    case BINARY_RESULTSET_ROW:
                        onRsRow(session);
                        break;
                    default:
                        throw new MycatException("错误包");
                }
                curPacketInf.markRead();
            }
        } catch (IOException e) {
            onRsFinish(session, false, e.getMessage());
            return;
        }
    }

    abstract void onRsColCount(T session);

    abstract void onRsColDef(T session);

    abstract void onRsRow(T session);

    abstract void onRsFinish(T session, boolean success, String msg) throws IOException;

    public enum ResultSetState {
        /**
         * 结果集默认值
         */
        RS_STATUS_NORMAL,

        /**
         * 结果集网络读取错误
         */
        RS_STATUS_READ_ERROR,

        /**
         * 结果集网络写入错误
         */
        RS_STATUS_WRITE_ERROR;
    }
}
