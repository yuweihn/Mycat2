package io.mycat.mycat2;

import io.mycat.mycat2.beans.MySQLMetaBean;
import io.mycat.mycat2.beans.MycatException;
import io.mycat.mycat2.beans.conf.SchemaBean;
import io.mycat.mycat2.net.MainMySQLNIOHandler;
import io.mycat.mycat2.tasks.AsynTaskCallBack;
import io.mycat.mycat2.tasks.BackendConCreateTask;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.SessionManager;
import io.mycat.proxy.buffer.BufferPool;
import io.mycat.util.ErrorCode;
import io.mycat.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.*;

/**
 * MySQL Session Manager (bakcend mysql connection manager)
 *
 * @author wuzhihui ,chenjunwen
 */
public class MySQLSessionManager implements SessionManager<MySQLSession> {
    protected static Logger logger = LoggerFactory.getLogger(MySQLSessionManager.class);
    protected final Map<MySQLMetaBean, ArrayList<MySQLSession>> idleSessionMap = new HashMap<>();
    protected final Map<MySQLMetaBean, ArrayList<MySQLSession>> allSessionMap = new HashMap<>();

    /**
     * 不存在mysql连接mycat
     *
     * @throws IOException
     * @author chenjunwen
     */
    @Override
    public MySQLSession createSessionForConnectedChannel(Object keyAttachement, BufferPool bufPool, Selector nioSelector, SocketChannel channel) throws IOException {
        throw new MycatException("Mysql server can not connect Mycat.");
    }

    /**
     * 获取所有的Session
     *chenjunwen
     * @param mysqlMetaBean
     * @return
     */
    public List<MySQLSession> getSessionsOfHost(MySQLMetaBean mysqlMetaBean) {
        if (mysqlMetaBean == null) {
            throw new MycatException("args of getSessionsOfHost errors: mysqlMetaBean" + mysqlMetaBean);
        }
        return allSessionMap.get(mysqlMetaBean);
    }

    /**
     * 获取闲置的Session
     *
     * @param mysqlMetaBean
     * @return
     */
    public List<MySQLSession> getIdleSessionsOfHost(MySQLMetaBean mysqlMetaBean) {
        if (mysqlMetaBean == null) {
            throw new MycatException("args of getIdleSessionsOfHost errors: mysqlMetaBean" + mysqlMetaBean);
        }
        return idleSessionMap.get(mysqlMetaBean);
    }

    /**
     * 清理DatasourceMetaBean相关的所有MySQL连接（关闭）
     *
     * @param dsMetaBean
     * @param reason
     */
    public void clearAndDestroyMySQLSession(MySQLMetaBean dsMetaBean, String reason) {
        if (dsMetaBean == null || StringUtil.isEmpty(reason)) {
            throw new MycatException("args of clearAndDestroyMySQLSession errors: dsMetaBean" + dsMetaBean + " reason:" + reason);
        }
        ArrayList<MySQLSession> allSessionList = allSessionMap.get(dsMetaBean);
        ArrayList<MySQLSession> idleSessionList = idleSessionMap.get(dsMetaBean);
        if (allSessionList != null) {
            for (MySQLSession f : allSessionList) {// 被某个Mycat连接所使用，则同时关闭Mycat连接
                if (f.getMycatSession() != null) {
                    logger.info("close Mycat session ,for it's using MySQL Con {} ", f);
                    f.getMycatSession().close(false, reason);
                }
                // 关闭MySQL连接
                f.close(false, reason);
            }
            // 清空MySQL连接池
            allSessionList.clear();
            allSessionMap.remove(dsMetaBean);

            idleSessionList.clear();
            idleSessionMap.remove(dsMetaBean);
        }
    }

    /**
     * 异步方式创建一个MySQL连接，成功或失败，都通过callBack回调通知给用户逻辑
     *
     * @param mySQLMetaBean 后端MySQL的信息
     * @param callBack      回调接口
     * @throws IOException
     */
    public void createMySQLSession(MySQLMetaBean mySQLMetaBean, AsynTaskCallBack<MySQLSession> callBack)
            throws IOException {
        if (mySQLMetaBean == null || callBack == null)
            throw new MycatException("args of createMySQLSession errors: mySQLMetaBean" + mySQLMetaBean + " callback:" + callBack);

        int backendCounts = 0;
        for (MycatReactorThread reActorthread : ProxyRuntime.INSTANCE.getMycatReactorThreads()) {
            List<MySQLSession> list = reActorthread.mysqlSessionMan.getSessionsOfHost(mySQLMetaBean);
            if (null != list) {
                backendCounts += list.size();
            }
        }
        if (backendCounts + 1 > mySQLMetaBean.getDsMetaBean().getMaxCon()) {
            ErrorPacket errPkg = new ErrorPacket();
            errPkg.packetId = 1;
            errPkg.errno = ErrorCode.ER_UNKNOWN_ERROR;
            errPkg.message = "backend connection is full for " + mySQLMetaBean.getDsMetaBean().getIp() + ":"
                    + mySQLMetaBean.getDsMetaBean().getPort();
            callBack.finished(null, null, false, errPkg);
            return;
        }
        MycatReactorThread curThread = (MycatReactorThread) Thread.currentThread();
        try {
            new BackendConCreateTask(curThread.getBufPool(), curThread.getSelector(), mySQLMetaBean, callBack);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            ErrorPacket errPkg = new ErrorPacket();
            errPkg.packetId = 1;
            errPkg.errno = ErrorCode.ER_UNKNOWN_ERROR;
            errPkg.message = "failed to create backend connection task for " + mySQLMetaBean.getDsMetaBean().getIp()
                    + ":" + mySQLMetaBean.getDsMetaBean().getPort();
            callBack.finished(null, null, false, errPkg);
        }
    }

    /**
     * 要求mySQLSession.isIdle()是闲置的
     * 从闲置的Session集合里移除
     * chenjunwen
     *
     * @param mySQLSession
     */
    public void removeIdleMySQLSession(MySQLSession mySQLSession) {
        if (!mySQLSession.channel().isConnected()) throw new MycatException("MySQLSession NotYetConnectedException");
        if (!mySQLSession.isIdle()) throw new MycatException("MySQLSession is not idle");
        ArrayList<MySQLSession> mySQLSessionList = idleSessionMap.get(mySQLSession.getMySQLMetaBean());
        if (mySQLSessionList != null) {
            mySQLSessionList.remove(mySQLSession);
        }
    }

    /**
     * 添加新的session,并根据是否闲置设置容器
     *
     * @param mySQLSession
     */
    public void addNewMySQLSession(MySQLSession mySQLSession) {
        if (mySQLSession == null || !mySQLSession.channel().isConnected())
            throw new MycatException("MySQLSession NotYetConnectedException");
        if (mySQLSession.getMySessionManager() != null) {
            throw new MycatException(mySQLSession + "has been in MySQLSessionManager");
        }
        addMySQLSession(this.allSessionMap, mySQLSession);
        if (mySQLSession.isIdle()) {
            addMySQLSession(this.idleSessionMap, mySQLSession);
        }
        mySQLSession.setSessionManager(this);
    }

    /**
     * 向容器添加Session
     *
     * @param sessionMap
     * @param mySQLSession
     */
    private void addMySQLSession(Map<MySQLMetaBean, ArrayList<MySQLSession>> sessionMap, MySQLSession mySQLSession) {
        ArrayList<MySQLSession> mySQLSessions = sessionMap.get(mySQLSession.getMySQLMetaBean());
        if (mySQLSessions == null) {
            ArrayList<MySQLSession> list = new ArrayList<>();
            list.add(mySQLSession);
            sessionMap.put(mySQLSession.getMySQLMetaBean(), list);
        } else {
            mySQLSessions.add(mySQLSession);
        }
    }

    /**
     * 向闲置的容器添加session
     *前提,
     * @param mySQLSession
     */
    public void addIdleMySQLSession(MySQLSession mySQLSession) {
        if (mySQLSession.channel().isConnected() && null == mySQLSession.getMycatSession())
            throw new MycatException("MySQLSession NotYetConnectedException");
        if (mySQLSession.isIdle()) throw new MycatException("MySQLSession is not idle");
        if (mySQLSession.getMySessionManager()==null) throw new MycatException("MySQLSession has not added");

        ArrayList<MySQLSession> mySQLSessionList = idleSessionMap.get(mySQLSession.getMySQLMetaBean());
        if (mySQLSessionList == null) {
            mySQLSessionList = new ArrayList<>(50);
            if (null != idleSessionMap.putIfAbsent(mySQLSession.getMySQLMetaBean(), mySQLSessionList)) {
                throw new MycatException(
                        "Duplicated MySQL Session ！！！，Please fix this Bug! Leader call you ! " + mySQLSession);
            }
        }
        mySQLSessionList.add(mySQLSession);

    }

    /**
     * 获取所有的Session对象
     * @return
     */
    @Override
    public Collection<MySQLSession> getAllSessions() {
        Collection<MySQLSession> result = new ArrayList<>();
        for (ArrayList<MySQLSession> sesLst : this.allSessionMap.values()) {
            result.addAll(sesLst);
        }

        return result;
    }

    /**
     * 从管理器中移除Session
     * @param theSession
     */
    public void removeSession(MySQLSession theSession) {
        if (theSession == null || !theSession.channel().isConnected())
            throw new MycatException("MySQLSession NotYetConnectedException");
        if (theSession.getMySessionManager() != null) {
            throw new MycatException(theSession + "has been in MySQLSessionManager");
        }
        ArrayList<MySQLSession> mysqlSessions = allSessionMap.get(theSession.getMySQLMetaBean());
        boolean find = false;
        if (mysqlSessions != null && mysqlSessions.remove(theSession)) {
            find = true;
        }
        ArrayList<MySQLSession> idleList = idleSessionMap.get(theSession.getMySQLMetaBean());
        if (idleList != null) {
            idleList.remove(theSession);
        }
        if (!find) {
            logger.warn("can't find MySQLSession  in map ,It's a bug ,please fix it ,{}", theSession);
        } else {
            logger.debug("removed MySQLSession  from  map .{} ", theSession);
        }

    }

    /**
     * 默认的Session处理句柄
     * @return
     */
    @Override
    public NIOHandler<MySQLSession> getDefaultSessionHandler() {
        return MainMySQLNIOHandler.INSTANCE;
    }

    @Override
    public int curSessionCount() {
        int count = 0;
        for (ArrayList<MySQLSession> sesLst : this.allSessionMap.values()) {
            count += sesLst.size();
        }
        return count;
    }

}
