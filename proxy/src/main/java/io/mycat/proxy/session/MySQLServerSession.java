package io.mycat.proxy.session;

import io.mycat.beans.mysql.MySQLErrorCode;
import io.mycat.beans.mysql.MySQLPayloadWriter;
import io.mycat.beans.mysql.MySQLServerStatusFlags;
import io.mycat.beans.mysql.packet.ErrorPacket;
import io.mycat.beans.mysql.packet.MySQLPacketSplitter;
import io.mycat.buffer.BufferPool;
import io.mycat.config.MySQLServerCapabilityFlags;
import io.mycat.proxy.MySQLPacketUtil;
import io.mycat.proxy.handler.MycatHandler.MycatSessionWriteHandler;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.packet.ErrorPacketImpl;
import io.mycat.proxy.packet.MySQLPacket;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.LinkedList;

/**
 * @author jamie12221 date 2019-05-08 00:06
 *
 * mysql server session 该接口实现服务器模式
 **/
public interface MySQLServerSession<T extends Session<T>> extends Session<T> {

    /**
     * 把队列的buffer写入通道,一个buffer是一个payload,写入时候转化成packet
     */
    static void writeToChannel(MySQLServerSession session) throws IOException {
        LinkedList<ByteBuffer> byteBuffers = session.writeQueue();
        ByteBuffer[] packetContainer = session.packetContainer();
        MySQLPacketSplitter packetSplitter = session.packetSplitter();
        long writed;
        do {
            writed = 0;
            if (byteBuffers.isEmpty()) {
                break;
            }
            ByteBuffer first = byteBuffers.peekFirst();

            if (first.position() == 0) {//一个全新的payload
                MycatMonitor.onFrontWrite(
                        session, first, 0, first.limit());
                packetSplitter.init(first.limit());
                packetSplitter.nextPacketInPacketSplitter();
                splitPacket(session, packetContainer, packetSplitter, first);
                assert packetContainer[0] != null;
                assert packetContainer[1] != null;
                writed = session.channel().write(packetContainer);
                if (first.hasRemaining()) {
                    return;
                } else {
                    continue;
                }
            } else {
                assert packetContainer[0] != null;
                assert packetContainer[1] != null;
                writed = session.channel().write(packetContainer);
                if (first.hasRemaining()) {
                    return;
                } else {
                    if (packetSplitter.nextPacketInPacketSplitter()) {
                        splitPacket(session, packetContainer, packetSplitter, first);
                        writed = session.channel().write(packetContainer);
                        if (first.hasRemaining()) {
                            return;
                        } else {
                            continue;
                        }
                    } else {
                        byteBuffers.removeFirst();
                        session.bufferPool().recycle(first);
                    }
                }
            }
        } while (writed > 0);
        if (writed == -1) {
            throw new ClosedChannelException();
        }
        if (byteBuffers.isEmpty() && session.isResponseFinished()) {
            session.writeFinished(session);
            return;
        }
    }

    /**
     * 生成packet
     */
    static void splitPacket(MySQLServerSession session, ByteBuffer[] packetContainer,
            MySQLPacketSplitter packetSplitter,
            ByteBuffer first) {
        int offset = packetSplitter.getOffsetInPacketSplitter();
        int len = packetSplitter.getPacketLenInPacketSplitter();
        setPacketHeader(session, packetContainer, len);

        first.position(offset).limit(len + offset);
        packetContainer[1] = first;
    }

    /**
     * 构造packet header
     */
    static void setPacketHeader(MySQLServerSession session, ByteBuffer[] packetContainer, int len) {
        ByteBuffer header = session.packetHeaderBuffer();
        header.position(0).limit(4);
        MySQLPacket.writeFixIntByteBuffer(header, 3, len);
        byte nextPacketId = session.getNextPacketId();
        header.put(nextPacketId);
        packetContainer[0] = header;
        header.flip();
    }

    BufferPool bufferPool();

    /**
     * 前端写入队列
     */
    LinkedList<ByteBuffer> writeQueue();

    /**
     * mysql 报文头 辅助buffer
     */
    ByteBuffer packetHeaderBuffer();

    /**
     * mysql 报文辅助buffer
     */
    ByteBuffer[] packetContainer();

    /**
     * 设置上下文packetId,用于响应生成
     */
    void setPakcetId(int packet);

    /**
     * ++packetId
     */
    byte getNextPacketId();

    /**
     * 获取 上下文错误信息,用于构造error Packet
     */
    String getLastMessage();

    /**
     * ok packet
     */
    long affectedRows();

    int setServerStatus(int s);

    int incrementWarningCount();

    /**
     * ok packet
     */
    long incrementAffectedRows();

    /**
     * ok eof
     */
    int getServerStatus();

    int setLastInsertId(int s);

    int getWarningCount();

    boolean isDeprecateEOF();

    long getLastInsertId();

    int getLastErrorCode();

    void setLastErrorCode(int errorCode);

    int charsetIndex();

    /**
     * 可能用于实现 reset connection命令
     */
    void resetSession();

    /**
     * 连接相关字符集
     */
    Charset charset();

    /**
     * 与客户端协商的服务器能力
     */
    int getCapabilities();

    /**
     * 前端写入事件是否结束
     */
    boolean isResponseFinished();

    /**
     * 设置响应结束,即payload写入结束
     */
    void setResponseFinished(boolean b);

    /**
     * 前端写入处理器可能有多种,此为设置服务器模式
     */
    void switchMySQLServerWriteHandler();

    /**
     * 写入文本结果集行
     */
    default void writeTextRowPacket(byte[][] row) {
        switchMySQLServerWriteHandler();
        byte[] bytes = MySQLPacketUtil.generateTextRow(row);
        writeBytes(bytes);
    }

    /**
     * 写入二进制结果集行
     */
    default void writeBinaryRowPacket(byte[][] row) {
        switchMySQLServerWriteHandler();
        byte[] bytes = MySQLPacketUtil.generateBinaryRow(row);
        writeBytes(bytes);
    }

    /**
     * 写入字段数
     */
    default void writeColumnCount(int count) {
        switchMySQLServerWriteHandler();
        byte[] bytes = MySQLPacketUtil.generateResultSetCount(count);
        writeBytes(bytes);
    }

    /**
     * 写入字段
     */
    default void writeColumnDef(String columnName, int type) {
        switchMySQLServerWriteHandler();
        byte[] bytes = MySQLPacketUtil
                .generateColumnDef(columnName, type, charsetIndex(), charset());
        writeBytes(bytes);
    }

    /**
     * 写入payload
     */
    default void writeBytes(byte[] payload) {
        switchMySQLServerWriteHandler();
        try {
            ByteBuffer buffer = bufferPool().allocate(payload);
            writeQueue().push(buffer);
            writeToChannel();
        } catch (Exception e) {
            this.close(false, setLastMessage(e));
        }
    }

    /**
     * 写入ok包,调用该方法,就指定响应已经结束
     */
    default void writeOkEndPacket() {
        switchMySQLServerWriteHandler();
        this.setResponseFinished(true);
        byte[] bytes = MySQLPacketUtil
                .generateOk(0, getWarningCount(), getServerStatus(), affectedRows(),
                        getLastInsertId(),
                        MySQLServerCapabilityFlags.isClientProtocol41(getCapabilities()),
                        MySQLServerCapabilityFlags.isKnowsAboutTransactions(getCapabilities()),
                        false, ""

                );
        writeBytes(bytes);
    }

    /**
     * 写入字段阶段技术报文,即字段包都写入后调用此方法
     */
    default void writeColumnEndPacket() {
        switchMySQLServerWriteHandler();
        if (isDeprecateEOF()) {
        } else {
            byte[] bytes = MySQLPacketUtil.generateEof(getWarningCount(), getServerStatus());
            writeBytes(bytes);
        }
    }

    /**
     * 结果集结束写入该报文,需要指定是否有后续的结果集和是否有游标
     */
    default void writeRowEndPacket(boolean hasMoreResult, boolean hasCursor) {
        switchMySQLServerWriteHandler();
        this.setResponseFinished(true);
        byte[] bytes;
        int serverStatus = getServerStatus();
        if (hasMoreResult) {
            serverStatus |= MySQLServerStatusFlags.MORE_RESULTS;
        }
        if (hasCursor) {
            serverStatus |= MySQLServerStatusFlags.CURSOR_EXISTS;
        }
        if (isDeprecateEOF()) {
            bytes = MySQLPacketUtil
                    .generateOk(0xfe, getWarningCount(), serverStatus, affectedRows(),
                            getLastInsertId(),
                            MySQLServerCapabilityFlags.isClientProtocol41(getCapabilities()),
                            MySQLServerCapabilityFlags.isKnowsAboutTransactions(getCapabilities()),
                            MySQLServerCapabilityFlags.isSessionVariableTracking(getCapabilities()),
                            getLastMessage());
        } else {
            bytes = MySQLPacketUtil.generateEof(getWarningCount(), getServerStatus());
        }
        writeBytes(bytes);
    }

    /**
     * 根据session的信息写入错误包,所以错误包的信息要设置session
     */
    default void writeErrorEndPacket() {
        int lastErrorCode = getLastErrorCode();
        if (lastErrorCode == 0) {
            lastErrorCode = MySQLErrorCode.ER_UNKNOWN_ERROR;
        }
        switchMySQLServerWriteHandler();
        this.setResponseFinished(true);
        byte[] bytes = MySQLPacketUtil
                .generateError(lastErrorCode, getLastMessage(), this.getServerStatus());
        writeBytes(bytes);
    }
    default void writeErrorEndPacket(ErrorPacketImpl packet) {
        int lastErrorCode = packet.getErrorCode();
        if (lastErrorCode == 0) {
            lastErrorCode = MySQLErrorCode.ER_UNKNOWN_ERROR;
        }
        switchMySQLServerWriteHandler();
        this.setResponseFinished(true);
        try(MySQLPayloadWriter writer = new MySQLPayloadWriter()){
            packet.writePayload(writer,getCapabilities());
            writeBytes(writer.toByteArray());
        }
    }

    default void writeEnd(){
        switchMySQLServerWriteHandler();
        this.setResponseFinished(true);
    }

    default void writeToChannel() throws IOException {
        writeToChannel(this);
    }

    default void writeErrorEndPacketBySyncInProcessError() {
        writeErrorEndPacketBySyncInProcessError(MySQLErrorCode.ER_UNKNOWN_ERROR);
    }

    default void writeErrorEndPacketBySyncInProcessError(int errorCode) {
        writeErrorEndPacketBySyncInProcessError(1, errorCode);
    }

    /**
     * 同步写入错误包,用于异常处理,一般错误包比较小,一次非阻塞写入就结束了,写入不完整尝试四次, 之后就会把mycat session关闭,简化错误处理
     */
    default void writeErrorEndPacketBySyncInProcessError(int packetId, int errorCode) {
        setLastErrorCode(errorCode);
        switchMySQLServerWriteHandler();
        this.setResponseFinished(true);
        byte[] bytes = MySQLPacketUtil
                .generateError(errorCode, getLastMessage(),
                        this.getCapabilities());
        byte[] bytes1 = MySQLPacketUtil.generateMySQLPacket(packetId, bytes);
        ByteBuffer message = ByteBuffer.wrap(bytes1);
        int counter = 0;
        try {
            SocketChannel channel = channel();
            if (channel.isOpen()) {
                while (message.hasRemaining() && counter < 4) {
                    channel().write(message);
                    counter++;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    MySQLPacketSplitter packetSplitter();

    /**
     * 前端写入处理器
     */
    enum WriteHandler implements MycatSessionWriteHandler {
        INSTANCE;

        @Override
        public void writeToChannel(MycatSession session) throws IOException {
            MySQLServerSession.writeToChannel(session);
        }

        @Override
        public void onException(MycatSession session, Exception e) {
            MycatMonitor.onMycatServerWriteException(session,e);
            session.resetPacket();
        }

    }


}
