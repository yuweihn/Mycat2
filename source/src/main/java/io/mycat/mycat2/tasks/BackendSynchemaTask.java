package io.mycat.mycat2.tasks;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mysql.MySQLPacketInf;
import io.mycat.mysql.packet.CommandPacket;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.util.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;

public class BackendSynchemaTask extends AbstractBackendIOTask<MySQLSession> {

    private static Logger logger = LoggerFactory.getLogger(BackendSynchemaTask.class);

    public BackendSynchemaTask(MySQLSession session, AsynTaskCallBack<MySQLSession> callBack) throws IOException {
        super(session, true);

        this.callBack = callBack;
        String targetDatabase = session.getMycatSession().getTargetDataNode().getDatabase();
        logger.info(" {} synchronize database from  {} to {} ", session, session.getDatabase(), targetDatabase);
        session.curPacketInf.getProxyBuffer().reset();
        CommandPacket packet = new CommandPacket();
        packet.packetId = 0;
        packet.command = MySQLCommand.COM_INIT_DB;
        packet.arg = targetDatabase.getBytes();
        packet.write(session.curPacketInf.getProxyBuffer());
        session.curPacketInf.getProxyBuffer().flip();
        session.curPacketInf.getProxyBuffer().readIndex = session.curPacketInf.getProxyBuffer().writeIndex;
        try {
            session.writeToChannel();
        } catch (ClosedChannelException e) {
            if (session.getMycatSession() != null) {
                session.close(false, "backend connection is closed!");
            }
            session.close(false, e.getMessage());
            return;
        } catch (Exception e) {
            String errmsg = "backend sync mycatSchema task Error. " + e.getMessage();
            errPkg = new ErrorPacket();
            errPkg.packetId = 1;
            errPkg.errno = ErrorCode.ER_UNKNOWN_ERROR;
            errPkg.message = errmsg;
            logger.error(errmsg);
            e.printStackTrace();
            this.finished(false);
        }
    }

    @Override
    public void onSocketRead(MySQLSession session) throws IOException {
        session.curPacketInf.getProxyBuffer().reset();

        try {
            if (!session.readFromChannel()) {
                return;
            }
        } catch (ClosedChannelException e) {
            session.close(false, e.getMessage());
            return;
        } catch (IOException e) {
            logger.debug("the Backend Synchema Task end ");
            String errmsg = "the backend sync mycatSchema task Error." + e.getMessage();
            errPkg = new ErrorPacket();
            errPkg.packetId = 1;
            errPkg.errno = ErrorCode.ER_UNKNOWN_ERROR;
            errPkg.message = errmsg;
            logger.error(errmsg);
            e.printStackTrace();
            this.finished(false);
            return;
        }
        if (!session.curPacketInf.readFully()) {
            return;
        }

        if (session.curPacketInf.head == MySQLPacket.OK_PACKET) {
            String database = session.getMycatSession().getTargetDataNode().getDatabase();
            session.setDatabase(database);
            logger.debug("the Backend Synchema Task end , to database {} ", database);
            this.finished(true);
        } else if (session.curPacketInf.head == MySQLPacket.ERROR_PACKET) {
            errPkg = new ErrorPacket();
            MySQLPacketInf curMQLPackgInf = session.curPacketInf;
            session.curPacketInf.getProxyBuffer().readIndex = curMQLPackgInf.startPos;
            errPkg.read(session.curPacketInf.getProxyBuffer());
            logger.debug("the Backend Synchema Task end ");
            logger.warn("backend state sync Error.Err No. " + errPkg.errno + "," + errPkg.message);
            this.finished(false);
        }


    }

}
