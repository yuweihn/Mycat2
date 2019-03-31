package io.mycat.mycat2.cmds;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.beans.conf.DNBean;
import io.mycat.mycat2.tasks.AsynTaskCallBack;
import io.mycat.mysql.MySQLPacketInf;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.ProxyRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.Iterator;


/**
 * 直接透传命令报文
 *
 * @author wuzhihui
 */
public class DirectPassthrouhCmd implements MySQLCommand {

    public static final DirectPassthrouhCmd INSTANCE = new DirectPassthrouhCmd();
    private static final Logger logger = LoggerFactory.getLogger(DirectPassthrouhCmd.class);

    @Override
    public boolean procssSQL(MycatSession session) throws IOException {
        MySQLPacketInf curPacketInf = session.curPacketInf;
        curPacketInf.payloadReader.changeToIterator();
        curPacketInf.multiPacketWriter.init(curPacketInf.payloadReader);
        return writeToMySQL(session, session.curPacketInf.multiPacketWriter);
    }


    /**
     *
     * @param session
     * @param writer
     * @return
     */
    public boolean writeToMySQL(MycatSession session, Iterator<ProxyBuffer> writer) {
        MySQLSession curBackend = session.getCurBackend();
        try {
            if (curBackend != null) {//@todo,需要检测后端session是否有效
                if (session.getTargetDataNode() == null) {
                    logger.warn("{} not specified SQL target DataNode ,so set to default dataNode ", session);
                    DNBean targetDataNode = ProxyRuntime.INSTANCE.getConfig().getMycatDataNodeMap()
                            .get(session.getMycatSchema().getDefaultDataNode());
                    session.setTargetDataNode(targetDataNode);
                }
                if (curBackend.synchronizedState(session.getTargetDataNode().getDatabase()) && curBackend.isActivated()) {
                    firstWriteToChannel(session, writer);
                } else {
                    // 同步数据库连接状态后回调
                    AsynTaskCallBack<MySQLSession> callback = (mysqlsession, sender, success, result) -> {
                        if (success) {
                            firstWriteToChannel(session, writer);
                        } else {
                            session.closeAllBackendsAndResponseError(success, ((ErrorPacket) result));
                        }
                    };
                    curBackend.syncAndCallback(callback);
                }
            } else {// 没有当前连接，尝试获取新连接
                session.getBackendAndCallBack((mysqlsession, sender, success, result) -> {
                    if (success) {
                        firstWriteToChannel(session, session.curPacketInf.multiPacketWriter);
                    } else {
                        session.closeAllBackendsAndResponseError(success, ((ErrorPacket) result));
                    }
                });
            }
        } catch (Throwable e) {
            session.closeAllBackendsAndResponseError(false, session.errorPacket("mycat session bind mysql session failly"));
        }
        return false;
    }

    private static void firstWriteToChannel(MycatSession s, Iterator<ProxyBuffer> writer) throws IOException {
        s.clearReadWriteOpts();
        MySQLSession curBackend = s.getCurBackend();
        MySQLPacketInf curPacketInf = curBackend.curPacketInf;
        if (writer.hasNext()) {
            curPacketInf.setProxyBuffer(writer.next());
            s.giveupOwner(SelectionKey.OP_WRITE);
            curBackend.writeToChannel();
        } else {
            s.closeAllBackendsAndResponseError(false, s.errorPacket("mycat session send SQL data to mysql session failly"));
        }
    }

    private static boolean continueWriteToChannel(MycatSession s, Iterator<ProxyBuffer> writer) throws IOException {
        Iterator<ProxyBuffer> multiPacketWriter = writer;
        MySQLSession curBackend = s.getCurBackend();
        MySQLPacketInf curPacketInf = curBackend.curPacketInf;
        boolean b = multiPacketWriter.hasNext();
        if (b) {
            curPacketInf.bufPool.recycle(curPacketInf.getProxyBuffer().getBuffer());
            curBackend.curPacketInf.setProxyBuffer(multiPacketWriter.next());
            curBackend.writeToChannel();
        } else {
            // 绝大部分情况下，前端把数据写完后端发送出去后，就等待后端返回数据了，
            // 向后端写入完成数据后，则从后端读取数据
            // 由于单工模式，在向后端写入完成后，需要从后端进行数据读取
            curPacketInf.reset();
            curBackend.curPacketInf.reset();
            curBackend.curPacketInf.setResponse();
            curBackend.change2ReadOpts();
        }
        return false;
    }

    @Override
    public boolean onBackendResponse(MySQLSession mySQLSession) throws IOException {
        if (!mySQLSession.readFromChannel()) {
            return false;
        }
        MySQLPacketInf packetInf = mySQLSession.curPacketInf;
        packetInf.directPassthrouhBuffer();
        MycatSession mycatSession = mySQLSession.getMycatSession();
        ProxyBuffer buffer = mySQLSession.curPacketInf.getProxyBuffer();
        buffer.flip();
        if (packetInf.isResponseFinished()) {
            mycatSession.takeOwner(SelectionKey.OP_READ);
            mySQLSession.setIdle(!packetInf.isInteractive());
        } else {
            mycatSession.takeOwner(SelectionKey.OP_WRITE);
        }
        mycatSession.writeToChannel();
        return false;
    }

    @Override
    public boolean onFrontWriteFinished(MycatSession mycatSession) throws IOException {
        // 判断是否结果集传输完成，决定命令是否结束，切换到前端读取数据
        // 检查当前已经结束，进行切换
        // 检查如果存在传输的标识，说明后传数据向前传传输未完成,注册后端的读取事件
        MySQLSession mySQLSession = mycatSession.getCurBackend();
        if (mySQLSession != null && mySQLSession.curPacketInf != null && !mySQLSession.curPacketInf.isResponseFinished()) {
            mycatSession.curPacketInf.getProxyBuffer().flip();
            mycatSession.giveupOwner(SelectionKey.OP_READ);
            return false;
        }
        // 当传输标识不存在，则说已经结束，则切换到前端的读取
        else {
            mycatSession.curPacketInf.reset();
            mycatSession.takeOwner(SelectionKey.OP_READ);
            return true;
        }
    }

    @Override
    public boolean onBackendWriteFinished(MySQLSession mySQLSession) throws IOException {
        MycatSession mycatSession = mySQLSession.getMycatSession();
        return continueWriteToChannel(mycatSession, mycatSession.curPacketInf.multiPacketWriter);

    }

    @Override
    public boolean onBackendClosed(MySQLSession session, boolean normal) {
        return true;
    }

    @Override
    public void clearResouces(MycatSession session, boolean sessionCLosed) {
        if (sessionCLosed) {
            session.unbindBackends();
        }
    }

}
