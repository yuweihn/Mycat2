package io.mycat.mycat2;

import io.mycat.mycat2.beans.MySQLMetaBean;
import io.mycat.mycat2.beans.MycatException;
import io.mycat.mycat2.beans.conf.DNBean;
import io.mycat.mycat2.beans.conf.SchemaBean;
import io.mycat.mycat2.cmds.LoadDataState;
import io.mycat.mycat2.sqlparser.BufferSQLContext;
import io.mycat.mycat2.sqlparser.BufferSQLParser;
import io.mycat.mycat2.sqlparser.TokenHash;
import io.mycat.mycat2.tasks.AsynTaskCallBack;
import io.mycat.mysql.AutoCommit;
import io.mycat.mysql.Capabilities;
import io.mycat.mysql.CapabilityFlags;
import io.mycat.mysql.MysqlNativePasswordPluginUtil;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.mysql.packet.HandshakePacket;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.buffer.BufferPool;
import io.mycat.util.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * 前端连接会话
 *
 * @author wuzhihui , cjw
 */
public class MycatSession extends AbstractMySQLSession {

    private static Logger logger = LoggerFactory.getLogger(MycatSession.class);
    private static Set<Byte> masterSqlList = new HashSet<>();

    static {
        masterSqlList.add(BufferSQLContext.INSERT_SQL);
        masterSqlList.add(BufferSQLContext.UPDATE_SQL);
        masterSqlList.add(BufferSQLContext.DELETE_SQL);
        masterSqlList.add(BufferSQLContext.REPLACE_SQL);
        masterSqlList.add(BufferSQLContext.SELECT_INTO_SQL);
        masterSqlList.add(BufferSQLContext.SELECT_FOR_UPDATE_SQL);
        masterSqlList.add(BufferSQLContext.CREATE_SQL);
        masterSqlList.add(BufferSQLContext.DROP_SQL);
        // TODO select lock in share mode 。 也需要走主节点 需要完善sql 解析器。
        masterSqlList.add(BufferSQLContext.LOAD_SQL);
        masterSqlList.add(BufferSQLContext.CALL_SQL);
        masterSqlList.add(BufferSQLContext.TRUNCATE_SQL);

        masterSqlList.add(BufferSQLContext.BEGIN_SQL);
        masterSqlList.add(BufferSQLContext.START_SQL); // TODO 需要完善sql 解析器。 将
        // start transaction
        // 分离出来。
        masterSqlList.add(BufferSQLContext.SET_AUTOCOMMIT_SQL);
    }

    private final ArrayList<MySQLSession> backends = new ArrayList<>(2);
    private int curBackendIndex = -1;
    // 当前SQL期待在哪个目标DataNode上执行
    private DNBean targetDataNode;
    // 所有处理cmd中,用来向前段写数据,或者后端写数据的cmd的
    private MySQLCommand curSQLCommand;
    public BufferSQLContext sqlContext = new BufferSQLContext();
    // 客户端连接的Mycat逻辑库
    private SchemaBean mycatSchema;
    public BufferSQLParser parser = new BufferSQLParser();
    private byte sqltype;

    public LoadDataState loadDataStateMachine = LoadDataState.NOT_LOAD_DATA;

    public byte getSqltype() {
        return sqltype;
    }

    public void setSqltype(byte sqltype) {
        this.sqltype = sqltype;
    }

    public MycatSession(BufferPool bufPool, Selector nioSelector, SocketChannel frontChannel, NIOHandler nioHandler) throws IOException {
        super(bufPool, nioSelector, frontChannel, nioHandler);

    }

    /**
     * 服务器能力@cjw
     * @return
     */
    public int getServerCapabilities() {
        int flag = 0;
        flag |= Capabilities.CLIENT_LONG_PASSWORD;
        flag |= Capabilities.CLIENT_FOUND_ROWS;
        flag |= Capabilities.CLIENT_LONG_FLAG;
        flag |= Capabilities.CLIENT_CONNECT_WITH_DB;
        // flag |= Capabilities.CLIENT_NO_SCHEMA;
        // boolean usingCompress = MycatServer.getInstance().getConfig()
        // .getSystem().getUseCompression() == 1;
        // if (usingCompress) {
        // flag |= Capabilities.CLIENT_COMPRESS;
        // }
        flag |= Capabilities.CLIENT_ODBC;
        flag |= Capabilities.CLIENT_LOCAL_FILES;
        flag |= Capabilities.CLIENT_IGNORE_SPACE;
        flag |= Capabilities.CLIENT_PROTOCOL_41;
        flag |= Capabilities.CLIENT_INTERACTIVE;
        // flag |= Capabilities.CLIENT_SSL;
        flag |= Capabilities.CLIENT_IGNORE_SIGPIPE;
        flag |= Capabilities.CLIENT_TRANSACTIONS;
        // flag |= ServerDefs.CLIENT_RESERVED;
        flag |= Capabilities.CLIENT_SECURE_CONNECTION;
        flag |= Capabilities.CLIENT_PLUGIN_AUTH;
        flag |= Capabilities.CLIENT_CONNECT_ATTRS;
        return flag;
    }


    /**
     * 关闭后端连接,同时向前端返回错误信息
     *
     * @param normal
     */
    public void closeAllBackendsAndResponseError(boolean normal, ErrorPacket error) {
        unbindAndCloseAllBackend(normal, error.message);
        takeBufferOwnerOnly();
        responseMySQLPacket(error);
    }

    /**
     * 关闭后端连接,同时向前端返回错误信息
     *
     * @param normal
     * @param errno
     * @param error
     * @throws IOException
     */
    public void closeAllBackendsAndResponseError(boolean normal, int errno, String error) {
        unbindAndCloseAllBackend(normal, error);
        takeBufferOwnerOnly();
        sendErrorMsg(errno, error);
    }

    private void unbindAndCloseAllBackend(boolean normal, String hint) {
        for (MySQLSession mySQLSession : this.backends) {
            logger.debug("close mysql connection {}", mySQLSession);
            mySQLSession.close(normal, hint);
        }
        this.unbindBackends();
    }

    /**
     * 向客户端响应 错误信息
     *
     * @param errno
     * @throws IOException
     */
    public void sendErrorMsg(int errno, String errMsg) {
        ErrorPacket errPkg = new ErrorPacket();
        errPkg.packetId = (byte)(this.curPacketInf.getCurrPacketId()+1);
        errPkg.errno = errno;
        errPkg.message = errMsg;
        responseMySQLPacket(errPkg);
    }

    /**
     * 绑定后端MySQL会话，同时作为当前的使用的后端连接(current backend) 主意：调用后，curBackendIndex会更新为
     * backend对应的Index！
     *
     * @param backend
     */
    public void bindBackend(MySQLSession backend) {
        if (backend == null || !backend.isIdle()||backend.getMycatSession()!=null) {
            throw new MycatException("Mycat Session Binding failed for MySQL Session");
        }
        ((MycatReactorThread) Thread.currentThread()).mysqlSessionMan.removeIdleMySQLSession(backend);
        this.curBackendIndex = putBackendMap(backend);
        logger.debug(" {} bind backConnection  for {}", this, backend);
    }

    /**
     *解除一个MySQLSession的绑定,标记当前使用Session在缓存中的下标
     * @param backend
     */
    public void unbindBackend(MySQLSession backend) {
        logger.debug(" {} unbind backConnection  for {}", this, backend);
        MySQLSession curSession = this.getCurBackend();
        if (backend==null||!backends.remove(backend)) {
            throw new MycatException("can't find backend " + backend);
        } else {
            unbindMySQLSession(backend);
        }
        // 调整curBackendIndex
        if (curSession == backend) {
            this.curBackendIndex = -1;
        } else if (curSession != null) {
            this.curBackendIndex = this.backends.indexOf(curSession);
        }
    }

    /**
     * 用于帮助解除MySQLSession绑定的函数,清除在mysqlSession里mycatSession的状态@cjw
     * @param mysql
     */
    private static void unbindMySQLSession(MySQLSession mysql) {
        mysql.setMycatSession(null);
        mysql.curPacketInf.useSharedBuffer(null);
        mysql.setCurBufOwner(true); // 设置后端连接 获取buffer 控制权
        mysql.setIdle(true);
        MySQLSessionManager mySessionManager = (MySQLSessionManager) (mysql.<MySQLSession>getMySessionManager());
        mySessionManager.addIdleMySQLSession(mysql);
    }
    /**
     * 将所有后端连接归还到ds中
     */
    public void unbindBackends() {
        for (MySQLSession mySQLSession : this.backends) {
            logger.debug("unbind mysql connection {}", mySQLSession);
            unbindMySQLSession(mySQLSession);
        }
        backends.clear();
        curBackendIndex = -1;
    }

    /**
     * 获取当前mycatSession使用的backend,会返回空指针@cjw
     * @return
     */
    public MySQLSession getCurBackend() {
        return (this.curBackendIndex == -1) ? null : this.backends.get(this.curBackendIndex);
    }

    /**
     * 使mycatSession获取Proxybuffer的所有权,同时使对应后端MysqlSession失去Proxybuffer的所有权@cjw
     */
    public void takeBufferOwnerOnly() {
        this.curPacketInf.setCurBufOwner(true);
        MySQLSession curBackend = getCurBackend();
        if (curBackend != null) {
            curBackend.setCurBufOwner(false);
        }
    }

    /**
     * 获取ProxyBuffer控制权，同时设置感兴趣的事件，如SocketRead，Write，只能其一
     *
     * @param intestOpts
     * @return
     */
    public void takeOwner(int intestOpts) {
        this.setCurBufOwner(true);
        if (intestOpts == SelectionKey.OP_READ) {
            this.change2ReadOpts();
        } else {
            this.change2WriteOpts();
        }
        MySQLSession curBackend = getCurBackend();
        if (curBackend != null) {
            curBackend.setCurBufOwner(false);
            curBackend.clearReadWriteOpts();
        }
    }

    /**
     * 放弃控制权，同时设置对端MySQLSession感兴趣的事件，如SocketRead，Write，只能其一
     *
     * @param intestOpts
     */
    public void giveupOwner(int intestOpts) {
        this.setCurBufOwner(false);
        this.clearReadWriteOpts();
        MySQLSession curBackend = getCurBackend();
        if (curBackend != null) {
            curBackend.setCurBufOwner(true);
            if (intestOpts == SelectionKey.OP_READ) {
                curBackend.change2ReadOpts();
            } else {
                curBackend.change2WriteOpts();
            }
        }

    }

    /**
     * 向前端发送数据报文,需要先确定为Write状态并确保写入位置的正确（frontBuffer.writeState)
     *
     * @param rawPkg
     * @throws IOException
     */
    public void answerFront(byte[] rawPkg) throws IOException {
        this.curPacketInf.getProxyBuffer().writeBytes(rawPkg);
        this.curPacketInf.getProxyBuffer().flip();
        this.curPacketInf.getProxyBuffer().readIndex = this.curPacketInf.getProxyBuffer().writeIndex;
        writeToChannel();
    }

    /**
     * 关闭mycat Session
     * 1.为了提高MySQLSession的利用效率,所以优先解除MySQLSession@cjw
     * 2.该函数被设计成可以幂等的,可以重复调用
     * 3.一旦调用该函数,应该彻底释放资源,同时mycatSession的handle不应该被io调用
     * @param normal
     * @param hint
     */
    public void close(boolean normal, String hint) {
        if (normal){
            this.unbindBackends();
        }else {
            this.unbindAndCloseAllBackend(normal,hint);
        }

        super.close(normal, hint);
    }

    /**
     *
     */
    @Override
    protected void doTakeReadOwner() {
        this.takeOwner(SelectionKey.OP_READ);
    }

    /**
     *
     * @return
     */
    private String getbackendName() {
        String backendName = null;
        switch (mycatSchema.getSchemaType()) {
            case DB_IN_ONE_SERVER:
                backendName = ProxyRuntime.INSTANCE.getConfig().getMycatDataNodeMap().get(mycatSchema.getDefaultDataNode())
                        .getReplica();
                break;
            case ANNOTATION_ROUTE:
                backendName = ProxyRuntime.INSTANCE.getConfig().getMycatDataNodeMap().get(mycatSchema.getDefaultDataNode())
                        .getReplica();
                break;
            case DB_IN_MULTI_SERVER:
                // 在 DB_IN_MULTI_SERVER
                // 模式中,如果不指定datanode以及Replica名字取得backendName,则使用默认的
                backendName = ProxyRuntime.INSTANCE.getConfig().getMycatDataNodeMap().get(mycatSchema.getDefaultDataNode())
                        .getReplica();
                break;
            default:
                break;
        }
        if (backendName == null) {
            throw new MycatException("the backendName must not be null");
        }
        return backendName;
    }


    public MySQLCommand getCurSQLCommand() {
        return curSQLCommand;
    }

    /**
     * 将后端连接放入到后端连接缓存中
     * 1.设置后端Session被占用
     * 2.mysqlSession设置mycatSession的引用
     * 3.mysqlSession使用mysession的Proxybuffer
     * 4.设置回调处理句柄
     * 5.添加到session缓存中
     *
     * @author chenjunwen
     * @param backend
     */
    private int putBackendMap(MySQLSession backend) {
  if (!backend.isIdle()||backend.getMycatSession()!=null) {

      throw new MycatException("backend is not idle");
  }
        backend.setIdle(false);
        backend.setMycatSession(this);
        backend.curPacketInf.useSharedBuffer(this.curPacketInf.getProxyBuffer());
        backend.setCurNIOHandler(this.getCurNIOHandler());
        this.backends.add(backend);
        int total = backends.size();
        logger.debug("add backend connection in mycatSession : {}, totals : {}  ,new bind is : {}", this, total,
                backend);
        return total - 1;
    }

    /**
     * 获取一个后端连接（新建或者重用当前的连接）,完成后回调通知。
     *
     * @return
     */
    public void getBackendAndCallBack(AsynTaskCallBack<MySQLSession> callback) throws IOException {
        ((MycatReactorThread) Thread.currentThread()).tryGetMySQLAndExecute(this, callback);

    }

    public DNBean getTargetDataNode() {
        return targetDataNode;
    }

    public void setTargetDataNode(DNBean targetDataNode) {
        logger.debug("{} set target datanode to {}", this, targetDataNode);
        this.targetDataNode = targetDataNode;
    }


    /**
     * 从后端连接中获取满足条件的连接 1. 主从节点 2. 空闲节点 返回-1，表示没找到，否则对应就是backends.get(i)
     */
    private int findMatchedMySQLSession(MySQLMetaBean targetMetaBean) {
        int findIndex = -1;
        // TODO 暂时不考虑分片情况下,分布式事务的问题。
        int total = backends.size();
        for (int i = 0; i < total; i++) {
            if (targetMetaBean.equals(backends.get(i).getMySQLMetaBean())) {
                findIndex = i;
                break;
            }
        }
        return findIndex;
    }

    /*
     * 判断后端连接 是否可以走从节点
     *
     * @return
     */
    private boolean canRunOnSlave() {
        // 静态注解情况下 走读写分离
        if (BufferSQLContext.ANNOTATION_BALANCE == sqlContext.getAnnotationType()) {
            final long balancevalue = sqlContext.getAnnotationValue(BufferSQLContext.ANNOTATION_BALANCE);
            if (TokenHash.MASTER == balancevalue) {
                return false;
            } else if (TokenHash.SLAVE == balancevalue) {
                return true;
            } else {
                logger.error("sql balance type is invalid, run on slave [{}]", sqlContext.getRealSQL(0));
            }
            return true;
        }

        // 非事务场景下，走从节点
        if (AutoCommit.ON == autoCommit) {
            return !masterSqlList.contains(sqlContext.getSQLType());
        } else {
            return false;
        }
    }

    public SchemaBean getMycatSchema() {
        return mycatSchema;
    }

    public void setMycatSchema(SchemaBean mycatSchema) {
        logger.info("{} set Mycat schema to {}", this, mycatSchema);
        this.mycatSchema = mycatSchema;
    }


    public void switchSQLCommand(MySQLCommand newCmd) {
        logger.debug("{} switch command from {} to  {} ", this, this.curSQLCommand, newCmd);
        this.curSQLCommand = newCmd;
    }

    public ErrorPacket errorPacket(String message){
        ErrorPacket errorPacket = new ErrorPacket();
        errorPacket.message = message;
        errorPacket.errno = ErrorCode.ER_UNKNOWN_ERROR;
        errorPacket.packetId = (byte)( this.curPacketInf.getCurrPacketId()+1);
        return errorPacket;
    }

}
