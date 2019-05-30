package io.mycat.mycat2;

import io.mycat.mycat2.beans.MySQLCharset;
import io.mycat.mysql.AutoCommit;
import io.mycat.mysql.Isolation;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.proxy.AbstractSession;
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.buffer.BufferPool;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

/**
 * 抽象的MySQL的连接会话
 *
 * @author wuzhihui
 */
public abstract class AbstractMySQLSession extends AbstractSession {
    /**
     * 字符集
     */
    public final MySQLCharset charSet = new MySQLCharset();
    /**
     * 用户
     */
    public String clientUser;
    /**
     * 事务隔离级别
     */
    public Isolation isolation = Isolation.REPEATED_READ;

    /**
     * 事务提交方式
     */
    public AutoCommit autoCommit = AutoCommit.ON;

    /**
     * 用来进行指定结束报文处理
     */
    public AbstractMySQLSession(BufferPool bufferPool, Selector selector, SocketChannel channel, NIOHandler nioHandler) throws IOException {
        this(bufferPool, selector, channel, SelectionKey.OP_READ, nioHandler);

    }

    public AbstractMySQLSession(BufferPool bufferPool, Selector selector, SocketChannel channel, int keyOpt, NIOHandler nioHandler)
            throws IOException {
        super(bufferPool, selector, channel, keyOpt, nioHandler);

    }

    /**
     * 回应客户端（front或Sever）OK 报文。
     *
     * @param pkg ，必须要是OK报文或者Err报文
     * @throws IOException
     */
    public void responseMySQLPacket(MySQLPacket pkg)  {
        try {
            this.curPacketInf.writeMySQLPacket(pkg);
            this.writeToChannel();
        } catch (IOException e) {
            this.close(false, "response write err , " + e.getMessage());
            logger.error("response write err , {} ", e);
            throw new RuntimeException(e);
        }
    }


    /**
     * 回应客户端（front或Sever）DEFAULT_OK_PACKET 报文。
     *
     * @param pkg ，必须要是OK报文或者Err报文
     * @throws IOException
     */
    public void responseMySQLPacket(byte[] pkg) throws IOException {
        try {
            this.curPacketInf.writeByteArray(pkg);
            this.writeToChannel();
        } catch (IOException e) {
            this.close(false, "response write err , " + e.getMessage());
            logger.error("response write err , {} ", e);
            throw new RuntimeException(e);
        }
    }


}
