package io.mycat.mycat2.tasks;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.beans.MySQLMetaBean;
import io.mycat.mysql.MySQLPacketInf;
import io.mycat.mysql.MysqlNativePasswordPluginUtil;
import io.mycat.mysql.packet.AuthPacket;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.mysql.packet.HandshakePacket;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.buffer.BufferPool;
import io.mycat.util.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

/**
 * 创建后端MySQL连接并负责完成登录认证的Processor
 *
 * @author wuzhihui
 */
public class BackendConCreateTask extends AbstractBackendIOTask<MySQLSession> {
    private static Logger logger = LoggerFactory.getLogger(BackendConCreateTask.class);
    private HandshakePacket handshake;
    private boolean welcomePkgReceived = false;
    private MySQLMetaBean mySQLMetaBean;
    private InetSocketAddress serverAddress;


    /**
     * 异步非阻塞模式创建MySQL连接，如果连接创建成功，需要把新连接加入到所在ReactorThread的连接池，则参数addConnectionPool需要设置为True
     *该方法获得已经连接上mysql 的session会不会马上加入到mySQLSessionMap,因为callback的方法里面需要马上用到这个session
     * @param bufPool
     * @param nioSelector   在哪个Selector上注册NIO事件（即对应哪个ReactorThread）
     * @param mySQLMetaBean
     * @param callBack      创建连接结束（成功或失败）后的回调接口
     * @throws IOException
     */
    public BackendConCreateTask(BufferPool bufPool, Selector nioSelector, MySQLMetaBean mySQLMetaBean,
                                AsynTaskCallBack<MySQLSession> callBack) throws IOException {
        super(null,false);
        String serverIP = mySQLMetaBean.getDsMetaBean().getIp();
        int serverPort = mySQLMetaBean.getDsMetaBean().getPort();
        logger.info("Connecting to backend MySQL Server " + serverIP + ":" + serverPort);
        serverAddress = new InetSocketAddress(serverIP, serverPort);
        SocketChannel backendChannel = SocketChannel.open();
        this.mySQLMetaBean = mySQLMetaBean;
        if (logger.isDebugEnabled()) {
            if (backendChannel.isConnected()) {
                logger.debug("MySQL client is not connected so start connecting" + backendChannel);
            }
        }
        BackendConCreateTask backendConCreateTask = this;
        MycatReactorThread mycatReactorThread = (MycatReactorThread) Thread.currentThread();
        AsynTaskCallBack<MySQLSession> task = (session, sender, success, result) -> {
            if (success) {
                mycatReactorThread.mysqlSessionMan.addNewMySQLSession(session);
                callBack.finished(session, mycatReactorThread.mysqlSessionMan, success, result);
            } else {
                callBack.finished(session, mycatReactorThread.mysqlSessionMan, false, result);
            }
        };
        backendConCreateTask.setCallback(task);
        backendChannel.configureBlocking(false);
        MySQLSession mySQLSession = new MySQLSession(bufPool, nioSelector, backendChannel, backendConCreateTask, mySQLMetaBean);
        backendConCreateTask.setSession(mySQLSession, false);
        backendChannel.connect(backendConCreateTask.getServerAddress());
    }

    @Override
    public void onWriteFinished(MySQLSession s) throws IOException {
        session.curPacketInf.reset();
        s.change2ReadOpts();
    }

    @Override
    public void onSocketRead(MySQLSession session) throws IOException {
        if (!session.readFromChannel()) {
            return;
        }
        if (!session.curPacketInf.readFully()){
            return;
        }
      //  MySQLPacketInf.simpleJudgeFullPacket()

        if (MySQLPacket.ERROR_PACKET == session.curPacketInf.head) {
            errPkg = new ErrorPacket();
            MySQLPacketInf curMQLPackgInf = session.curPacketInf;
            session.curPacketInf.getProxyBuffer().readIndex = curMQLPackgInf.startPos;
            errPkg.read(session.curPacketInf.getProxyBuffer());
            logger.warn("backend authed failed. Err No. " + errPkg.errno + "," + errPkg.message);
            this.finished(false);
            return;
        }

        if (!welcomePkgReceived) {
            handshake = new HandshakePacket();
            handshake.read(this.session.curPacketInf.getProxyBuffer());

            // 设置字符集编码
            // int charsetIndex = (handshake.characterSet & 0xff);
            int charsetIndex = handshake.characterSet;
            // 发送应答报文给后端
            AuthPacket packet = new AuthPacket();
            packet.packetId = 1;
            packet.capabilities = MySQLSession.getClientCapabilityFlags().value;
            packet.maxPacketSize = 1024 * 1000;
            packet.characterSet = (byte) charsetIndex;
            packet.username = mySQLMetaBean.getDsMetaBean().getUser();
            packet.password = MysqlNativePasswordPluginUtil.scramble411(mySQLMetaBean.getDsMetaBean().getPassword(),
                    handshake.authPluginDataPartOne + handshake.authPluginDataPartTwo);
            packet.authPluginName = MysqlNativePasswordPluginUtil.PROTOCOL_PLUGIN_NAME;
            // SchemaBean mycatSchema = session.mycatSchema;
            // 创建连接时，默认不主动同步数据库
            // if(mycatSchema!=null&&mycatSchema.getDefaultDN()!=null){
            // packet.database = mycatSchema.getDefaultDN().getDatabase();
            // }

            // 不透传的状态下，需要自己控制Buffer的状态，这里每次写数据都切回初始Write状态
            session.curPacketInf.getProxyBuffer().reset();
            packet.write(session.curPacketInf.getProxyBuffer());
            session.curPacketInf.getProxyBuffer().flip();
            // 不透传的状态下， 自己指定需要写入到channel中的数据范围
            // 没有读取,直接透传时,需要指定 透传的数据 截止位置
            session.curPacketInf.getProxyBuffer().readIndex = session.curPacketInf.getProxyBuffer().writeIndex;
            session.writeToChannel();
            welcomePkgReceived = true;
        } else {
            // 认证结果报文收到
            if (session.curPacketInf.head == MySQLPacket.OK_PACKET) {
                logger.debug("backend authed suceess ");
                this.finished(true);
            }
        }
    }

    @Override
    public void onConnect(SelectionKey theKey, MySQLSession userSession, boolean success, String msg)
            throws IOException {
        if (logger.isDebugEnabled()) {
            String logInfo = success ? " backend connect success " : "backend connect failed " + msg;
            logger.debug("sessionId = {}," + logInfo + " {}:{}", userSession.getSessionId(),
                    userSession.getMySQLMetaBean().getDsMetaBean().getIp(),
                    userSession.getMySQLMetaBean().getDsMetaBean().getPort());
        }
        if (success) {
            InetSocketAddress serverRemoteAddr = (InetSocketAddress) userSession.channel.getRemoteAddress();
            InetSocketAddress serverLocalAddr = (InetSocketAddress) userSession.channel.getLocalAddress();
            userSession.addr = "local port:" + serverLocalAddr.getPort() + ",remote " + serverRemoteAddr.getHostString()
                    + ":" + serverRemoteAddr.getPort();
            userSession.channelKey.interestOps(SelectionKey.OP_READ);
        } else {
            errPkg = new ErrorPacket();
            errPkg.packetId = 1;
            errPkg.errno = ErrorCode.ERR_CONNECT_SOCKET;
            errPkg.message = "backend connect failed " + msg;
            //新建连接失败，此时MySQLSession并未绑定到MycatSession上，因此需要单独关闭连接，从MySQLSessionManager中移除
            userSession.close(true, msg);
            finished(false);
        }

    }

    public HandshakePacket getHandshake() {
        return handshake;
    }

    public boolean isWelcomePkgReceived() {
        return welcomePkgReceived;
    }

    public MySQLMetaBean getMySQLMetaBean() {
        return mySQLMetaBean;
    }


    public InetSocketAddress getServerAddress() {
        return serverAddress;
    }
}
