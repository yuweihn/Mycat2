package io.mycat.mysql;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.beans.MycatException;
import io.mycat.mycat2.beans.conf.ProxyConfig;
import io.mycat.mycat2.cmds.judge.JudgeUtil;
import io.mycat.mysql.packet.EOFPacket;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.mysql.packet.OKPacket;
import io.mycat.proxy.ConfigEnum;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.ProxyReactorThread;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.buffer.BufferPool;
import io.mycat.util.ParseUtil;
import io.mycat.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import static io.mycat.mysql.MySQLPayloadType.*;

/*
cjw
294712221@qq.com
 */
public class MySQLPacketInf {
    public int head;
    public int startPos;
    public int endPos;
    public int pkgLength;
    public int remainsBytes;//还有多少字节才结束，仅对跨多个Buffer的MySQL报文有意义（crossBuffer=true)
    public boolean crossPacket = false;
    public PacketType packetType = PacketType.SHORT_HALF;

    public void setProxyBuffer(ProxyBuffer proxyBuffer) {
        this.proxyBuffer = proxyBuffer;
        multiPackets.set(0,proxyBuffer);
    }

    public ProxyBuffer getProxyBuffer() {
        return proxyBuffer;
    }

    private ProxyBuffer proxyBuffer;
    private final List<ProxyBuffer> multiPackets = new ArrayList<>();

    public BufferPool bufPool;

    protected boolean referedBuffer;//是否多个Session共用同一个Buffer
    protected boolean curBufOwner = true;//是否多个Session共用同一个Buffer时，当前Session是否暂时获取了Buffer独家使用权，即独占Buffer
    public final PacketListToPayloadReader payloadReader = new PacketListToPayloadReader(multiPackets);
    public final MultiPacketWriter multiPacketWriter = new MultiPacketWriter(multiPackets);

    private int sqlType;
    public int prepareFieldNum = 0;
    public int prepareParamNum = 0;
    public long columnCount = 0;
    public int serverStatus = 0;
    private byte packetId = 0;
    public ComQueryState state = ComQueryState.QUERY_PACKET;
    public MySQLPayloadType mysqlPacketType = MySQLPayloadType.UNKNOWN;
    private boolean hasResolvePayloadType = false;

    protected long lastLargeMessageTime;

    //初始化设置,与系统配置有关 @cjw
    protected final boolean CLIENT_DEPRECATE_EOF;
    protected final CapabilityFlags capabilityFlags;

    public static boolean simpleJudgeFullPacket(ProxyBuffer proxyBuffer) {
        int offset = proxyBuffer.readIndex;   // 读取的偏移位置
        int limit = proxyBuffer.writeIndex;   // 读取的总长度
        int totalLen = limit - offset;      // 读取当前的总长度
        if (totalLen < 4) {
            return false;
        }
        int payloadLength = ParseUtil.getPayloadLength(proxyBuffer.getBuffer(), offset);
        return totalLen >= payloadLength + 4;
    }

    public boolean isResponseFinished() {
        return this.state == ComQueryState.COMMAND_END;
    }

    public static boolean needExpandCapacity(ProxyBuffer proxyBuffer) {
        ByteBuffer buffer = proxyBuffer.getBuffer();
        return buffer.capacity() == buffer.position();
    }

    public static void simpleAdjustCapacityProxybuffer(ProxyBuffer proxyBuffer, int len) {
        int offset = proxyBuffer.readIndex;   // 读取的偏移位置
        int limit = proxyBuffer.writeIndex;   // 读取的总长度
        int totalLen = limit - offset;      // 读取当前的总长度
        ByteBuffer buffer = proxyBuffer.getBuffer();
        ProxyReactorThread proxyReactorThread = (ProxyReactorThread) Thread.currentThread();
        ByteBuffer allocate = proxyReactorThread.getBufPool().allocate(len);
        int olimit = buffer.limit();
        int oPosition = buffer.position();
        buffer.position(0);
        buffer.limit(buffer.capacity());
        allocate.put(buffer);
        proxyReactorThread.getBufPool().recycle(buffer);
        allocate.position(oPosition);
        allocate.limit(len);
        proxyBuffer.setBuffer(allocate);
    }

    public boolean resolveCrossBufferFullPayload() {
        PacketType type = resolveMySQLPacket();
        if (!this.crossPacket && (type == PacketType.FINISHED_CROSS || type == PacketType.FULL)) {
            this.markRead();
            return true;//true
        } else if (type == PacketType.LONG_HALF) {
            if (crossBuffer()) {
                this.markRead();
            }
            return false;//false
        } else if (type == PacketType.SHORT_HALF) {
            return false;//false
        } else {
            this.markRead();
            return false;//false
        }
    }

    public PacketType resolveMySQLPacket() {
        MySQLPacketInf packetInf = this;
        int offset = proxyBuffer.readIndex;   // 读取的偏移位置
        int limit = proxyBuffer.writeIndex;   // 读取的总长度
        int totalLen = limit - offset;      // 读取当前的总长度
        ByteBuffer buffer = proxyBuffer.getBuffer();
        switch (packetInf.packetType) {
            case SHORT_HALF:
            case FULL:
            case FINISHED_CROSS: {
                if (totalLen > 3) {//totalLen >= 4
                    hasResolvePayloadType = false;
                    byte packetId = buffer.get(offset + 3);
                    checkRequestStart(packetId);
                    int payloadLength = ParseUtil.getPayloadLength(buffer, offset);
                    boolean isCrossPacket = this.crossPacket;
                    this.crossPacket = payloadLength == 0xffffff;
                    if (this.crossPacket) {
                        packetInf.pkgLength = 0xffffff;
                    } else {
                        packetInf.pkgLength = payloadLength + 4;
                    }
                    boolean isPacketFinished = totalLen >= packetInf.pkgLength;
                    if (isCrossPacket) {
                        return resolveLongHalf(offset, limit, totalLen);
                    } else {
                        if (totalLen > 4) {
                            return resolveShort2FullOrLongHalf(offset, limit, buffer, isPacketFinished);
                        } else if (totalLen == 4 && packetInf.pkgLength == 4) {
                            this.mysqlPacketType = MySQLPayloadType.UNKNOWN;
                            this.crossPacket = false;
                            packetInf.endPos = 4;
                            return packetInf.packetType = PacketType.FULL;
                        } else {
                            return resolveShortHalf();
                        }
                    }
                } else {
                    return resolveShortHalf();
                }
            }
            case LONG_HALF:
                return resolveLongHalf(offset, limit, totalLen);
            case REST_CROSS:
                return resolveRestCross(offset, limit, totalLen);
            default:
                throw new RuntimeException("unknown state!");
        }
    }

    private PacketType resolveShortHalf() {
        this.crossPacket = false;
        this.mysqlPacketType = UNKNOWN;
        return this.change2ShortHalf();
    }

    private PacketType resolveShort2FullOrLongHalf(int offset, int limit, ByteBuffer buffer, boolean isPacketFinished) {
        this.head = buffer.get(offset + 4) & 0xff;
        checkNeedFull(this);
        this.startPos = offset;
        if (isPacketFinished) {
            this.endPos = offset + this.pkgLength;
            if (!this.crossPacket) {
                resolvePayloadType(true);
            }
            return this.packetType = PacketType.FULL;
        } else {
            if (!this.state.needFull) {
                resolvePayloadType(isPacketFinished);
            }
            this.endPos = limit;
            return this.packetType = PacketType.LONG_HALF;
        }
    }


    public boolean isInteractive() {
        return this.state != ComQueryState.COMMAND_END || JudgeUtil.hasTrans(serverStatus) || JudgeUtil.hasFatch(serverStatus);
    }

    private PacketType resolveLongHalf(int offset, int limit, int totalLen) {
        MySQLPacketInf packetInf = this;
        packetInf.startPos = offset;
        if (totalLen >= packetInf.pkgLength) {
            packetInf.endPos = offset + packetInf.pkgLength;
            if (this.state.needFull && !this.crossPacket) {
                resolvePayloadType(true);
            }
            return packetInf.packetType = PacketType.FULL;
        } else {
            packetInf.endPos = limit;
            return packetInf.packetType = PacketType.LONG_HALF;
        }
    }

    private PacketType resolveRestCross(int offset, int limit, int totalLen) {
        MySQLPacketInf packetInf = this;
        if (packetInf.remainsBytes <= totalLen) {// 剩余报文结束
            packetInf.endPos = offset + packetInf.remainsBytes;
            offset += packetInf.remainsBytes; // 继续处理下一个报文
            packetInf.proxyBuffer.readIndex = offset;
            packetInf.remainsBytes = 0;
            return packetInf.packetType = PacketType.FINISHED_CROSS;
        } else {// 剩余报文还没读完，等待下一次读取
            packetInf.startPos = 0;
            packetInf.remainsBytes -= totalLen;
            packetInf.endPos = limit;
            packetInf.proxyBuffer.readIndex = packetInf.endPos;
            return packetInf.packetType = PacketType.REST_CROSS;
        }
    }

    public boolean crossBuffer() {
        MySQLPacketInf packetInf = this;
        if (packetInf.packetType == PacketType.LONG_HALF && !packetInf.state.needFull) {
            packetInf.proxyBuffer.readIndex = packetInf.endPos;
            packetInf.updateState(packetInf.head, packetInf.startPos, packetInf.endPos, packetInf.pkgLength,
                    packetInf.pkgLength - (packetInf.endPos - packetInf.startPos), PacketType.REST_CROSS, packetInf.proxyBuffer);
            return true;
        } else {
            return false;
        }
    }

    private void checkNeedFull(MySQLPacketInf packetInf) {
        if ((state == ComQueryState.RESULTSET_ROW) && (packetInf.head == 0xfe) && packetInf.pkgLength < 0xffffff) {
            this.state = ComQueryState.RESULTSET_ROW_END;
        }
    }

    private void checkRequestStart(byte packetId) {
        if (packetId == 0) {
            this.state = ComQueryState.QUERY_PACKET;
            if (logger.isDebugEnabled()) {
                logger.debug("because packetId is " + packetId + " so start QUERY_PACKET");
            }
            this.packetId = 1;
        }
    }

    private void checkPacketId(byte packetId) {
//        if (this.state != ComQueryState.DO_NOT) {
//            if (this.packetId != packetId) {
////                throw new RuntimeException("packetId should be " + packetId + " that is not match " + packetId);
//            } else {
//            }
//            ++this.packetId;
//        }
//        if (packetId == 0) {
//            System.out.println("-------------------------------------------------------------------------");
//            ;
//        }
    }


    public void resolvePayloadType(boolean isPacketFinished) {
        MySQLPacketInf packetInf = this;
        if (hasResolvePayloadType) {
            return;
        }
        hasResolvePayloadType = true;
        int head = packetInf.head;
        switch (state) {
            case QUERY_PACKET: {
                if (!isPacketFinished) throw new RuntimeException("unknown state!");
                if (head == 18) {
//                    ProxyBuffer buffer = curPacketInf.proxyBuffer;
//                    int startIndex = buffer.readIndex;
//                    int endIndex = buffer.writeIndex;
//                    int sendLongDataSize = endIndex - startIndex - (4+7);//packetHead大小+非data数据大小
//                    buffer.readIndex = curPacketInf.startPos + 5;
//                    int statementId = (int) buffer.readFixInt(4);
//                    int paramId = (int) buffer.readFixInt(2);
                    this.sqlType = head;
                    this.mysqlPacketType = SEND_LONG_DATA;
                    state = ComQueryState.FIRST_PACKET;
                    return;
                }
                this.sqlType = head;
//                if (head != COM_STMT_CLOSE) {
//                    state = ComQueryState.FIRST_PACKET;
//                }
                return;
            }
            case FIRST_PACKET: {
                if (!isPacketFinished) throw new RuntimeException("unknown state!");
                if (head == 0xff) {
                    this.mysqlPacketType = MySQLPayloadType.ERROR;
                    state = ComQueryState.COMMAND_END;
                } else if (head == 0x00) {
                    if (sqlType == MySQLCommand.COM_STMT_PREPARE && packetInf.pkgLength == 16 && packetInf.packetId == 1) {
                        resolvePrepareOkPacket(isPacketFinished);
                        return;
                    } else {
                        this.mysqlPacketType = MySQLPayloadType.OK;
                        this.serverStatus = OKPacket.readServerStatus(packetInf.proxyBuffer, capabilityFlags);
                        state = ComQueryState.COMMAND_END;
                        return;
                    }
                } else if (head == 0xfb) {
                    state = ComQueryState.LOCAL_INFILE_FILE_CONTENT;
                    this.mysqlPacketType = LOCAL_INFILE_REQUEST;
                    return;
                } else if (head == 0xfe) {
                    this.mysqlPacketType = EOF;
                    this.serverStatus = EOFPacket.readStatus(packetInf.proxyBuffer);
                    state = ComQueryState.COMMAND_END;
                    return;
                } else {
                    ProxyBuffer proxyBuffer = packetInf.proxyBuffer;
                    columnCount = proxyBuffer.getLenencInt(packetInf.startPos + 4);
                    this.mysqlPacketType = MySQLPayloadType.COLUMN_COUNT;
                    state = ComQueryState.COLUMN_DEFINITION;
                }
                return;
            }
            case COLUMN_DEFINITION: {
                --columnCount;
                if (columnCount == 0) {
                    this.state = !this.CLIENT_DEPRECATE_EOF ? ComQueryState.COLUMN_END_EOF : ComQueryState.RESULTSET_ROW;
                }
                this.mysqlPacketType = MySQLPayloadType.COLUMN_DEFINITION;
                return;
            }
            case COLUMN_END_EOF: {
                if (!isPacketFinished) throw new RuntimeException("unknown state!");
                this.serverStatus = EOFPacket.readStatus(packetInf.proxyBuffer);
                this.mysqlPacketType = EOF;
                this.state = ComQueryState.RESULTSET_ROW;
                return;
            }
            case RESULTSET_ROW: {
                if (head == 0x00) {
                    //binary resultset row
                    this.mysqlPacketType = MySQLPayloadType.BINARY_RESULTSET_ROW;
                } else if (head == 0xfe && packetInf.pkgLength < 0xffffff) {
                    resolveResultsetRowEnd(isPacketFinished);
                } else if (head == MySQLPacket.ERROR_PACKET) {
                    this.mysqlPacketType = MySQLPayloadType.ERROR;
                    state = ComQueryState.COMMAND_END;
                } else {
                    this.mysqlPacketType = MySQLPayloadType.TEXT_RESULTSET_ROW;
                    //text resultset row
                }
                break;
            }
            case RESULTSET_ROW_END:
                resolveResultsetRowEnd(isPacketFinished);
                break;
            case PREPARE_FIELD:
            case PREPARE_FIELD_EOF:
            case PREPARE_PARAM:
            case PREPARE_PARAM_EOF:
                resolvePrepareResponse(packetInf.proxyBuffer, head, isPacketFinished);
                return;
            case LOCAL_INFILE_FILE_CONTENT:
                if (packetInf.pkgLength == 4) {
                    state = ComQueryState.LOCAL_INFILE_OK_PACKET;
                    this.mysqlPacketType = LOCAL_INFILE_EMPTY_PACKET;
                    return;
                } else {
                    state = ComQueryState.LOCAL_INFILE_FILE_CONTENT;
                    this.mysqlPacketType = LOCAL_INFILE_CONTENT_OF_FILENAME;
                    return;
                }
            case LOCAL_INFILE_OK_PACKET:
                if (!isPacketFinished) throw new RuntimeException("unknown state!");
                this.mysqlPacketType = MySQLPayloadType.OK;
                this.serverStatus = OKPacket.readServerStatus(packetInf.proxyBuffer, capabilityFlags);
                state = ComQueryState.COMMAND_END;
                return;
            case COMMAND_END:
                return;
            default: {
                if (!isPacketFinished) {
                    throw new RuntimeException("unknown state!");
                } else {

                }
            }
        }
    }

    private void resolveResultsetRowEnd(boolean isPacketFinished) {
        if (!isPacketFinished) throw new RuntimeException("unknown state!");
        if (CLIENT_DEPRECATE_EOF) {
            this.mysqlPacketType = MySQLPayloadType.OK;
            serverStatus = OKPacket.readServerStatus(proxyBuffer, capabilityFlags);
        } else {
            this.mysqlPacketType = EOF;
            serverStatus = EOFPacket.readStatus(proxyBuffer);
        }
        if (JudgeUtil.hasMoreResult(serverStatus)) {
            state = ComQueryState.FIRST_PACKET;
        } else {
            state = ComQueryState.COMMAND_END;
        }
    }

    private void resolvePrepareOkPacket(boolean isPacketFinished) {
        if (!isPacketFinished) throw new RuntimeException("unknown state!");
        ProxyBuffer buffer = proxyBuffer;
        buffer.readIndex = startPos + 9;
        this.prepareFieldNum = (int) buffer.readFixInt(2);
        this.prepareParamNum = (int) buffer.readFixInt(2);
        this.mysqlPacketType = MySQLPayloadType.PREPARE_OK;
        if (this.prepareFieldNum == 0 && this.prepareParamNum == 0) {
            state = ComQueryState.COMMAND_END;
            return;
        } else if (this.prepareFieldNum > 0) {
            state = ComQueryState.PREPARE_FIELD;
            return;
        }
        if (this.prepareParamNum > 0) {
            state = ComQueryState.PREPARE_PARAM;
            return;
        }
        throw new RuntimeException("unknown state!");
    }

    private void resolvePrepareResponse(ProxyBuffer proxyBuf, int head, boolean isPacketFinished) {
        if (!isPacketFinished) throw new RuntimeException("unknown state!");
        if (prepareFieldNum > 0 && (state == ComQueryState.PREPARE_FIELD)) {
            prepareFieldNum--;
            this.mysqlPacketType = MySQLPayloadType.COLUMN_DEFINITION;
            this.state = ComQueryState.PREPARE_FIELD;
            if (prepareFieldNum == 0) {
                if (!CLIENT_DEPRECATE_EOF) {
                    this.state = ComQueryState.PREPARE_FIELD_EOF;
                } else if (prepareParamNum > 0) {
                    this.state = ComQueryState.PREPARE_PARAM;
                } else {
                    this.state = ComQueryState.COMMAND_END;
                }
            }
            return;
        } else if (this.state == ComQueryState.PREPARE_FIELD_EOF && head == 0xFE) {
            this.serverStatus = EOFPacket.readStatus(proxyBuf);
            this.mysqlPacketType = EOF;
            this.state = ComQueryState.PREPARE_PARAM;
            return;
        } else if (prepareParamNum > 0 && this.state == ComQueryState.PREPARE_PARAM) {
            prepareParamNum--;
            this.mysqlPacketType = MySQLPayloadType.COLUMN_DEFINITION;
            this.state = ComQueryState.PREPARE_PARAM;
            if (prepareParamNum == 0) {
                if (!CLIENT_DEPRECATE_EOF) {
                    state = ComQueryState.PREPARE_PARAM_EOF;
                    return;
                } else {
                    state = ComQueryState.COMMAND_END;
                    return;
                }
            } else {
                return;
            }
        } else if (prepareFieldNum == 0 && prepareParamNum == 0 && !CLIENT_DEPRECATE_EOF && ComQueryState.PREPARE_PARAM_EOF == state && head == 0xFE) {
            this.serverStatus = EOFPacket.readStatus(proxyBuf);
            this.mysqlPacketType = EOF;
            state = ComQueryState.COMMAND_END;
            return;
        }
        throw new RuntimeException("unknown state!");
    }


    public void useDirectPassthrouhBuffer() {
        state = ComQueryState.FIRST_PACKET;
    }

    public boolean readFully() {
        MySQLPacketInf inf = this;
        resolveMySQLPacket();
        int startPos = inf.startPos + 4;
        int endPos = inf.endPos;
        while (true) {
            switch (inf.packetType) {
                case FULL: {
                    if (!inf.proxyBuffer.getBuffer().hasRemaining()){
                        ProxyBuffer proxyBuffer = new ProxyBuffer(bufPool.allocate());
                        inf.payloadReader.addBuffer(proxyBuffer);
                        inf.proxyBuffer = proxyBuffer;
                    }
                    return !crossPacket;
                }
                default:
                    ensureFreeSpaceOfReadBuffer();
                    return false;
            }
        }

    }

    public MySQLPacketInf directPassthrouhBuffer() {
        MySQLPacketInf packetInf = this;
        while (true) {
            if (packetInf.resolveCrossBufferFullPayload()) {
                return this;
            } else {
                if (this.state.isNeedFull() && packetInf.needExpandBuffer()) {
                    simpleAdjustCapacityProxybuffer(packetInf.proxyBuffer, packetInf.endPos + packetInf.pkgLength);
                }
                break;
            }
        }
        return packetInf;
    }

    public int getCurrPacketId() {
        return packetId - 1;
    }

    private void updateState(int head, int startPos, int endPos, int pkgLength, int remainsBytes, PacketType packetType, ProxyBuffer proxyBuffer
    ) {
        this.head = head;
        this.startPos = startPos;
        this.endPos = endPos;
        this.pkgLength = pkgLength;
        this.remainsBytes = remainsBytes;
        this.packetType = packetType;
        this.proxyBuffer = proxyBuffer;
    }

    public boolean needExpandBuffer() {
        return pkgLength > proxyBuffer.getBuffer().capacity();
    }

    public PacketType change2ShortHalf() {
        this.updateState(0, 0, 0, 0, 0, PacketType.SHORT_HALF, proxyBuffer);
        return PacketType.SHORT_HALF;
    }

    public void markRead() {
        if (packetType == PacketType.FULL || packetType == PacketType.REST_CROSS || packetType == PacketType.FINISHED_CROSS) {
            this.proxyBuffer.readIndex = endPos;
        } else {
            throw new UnsupportedOperationException("markRead is only in FULL or REST_CROSS or FINISHED_CROSS");
        }
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", MySQLPacketInf.class.getSimpleName() + "[", "]")
                .add("head=" + head)
                .add("startPos=" + startPos)
                .add("endPos=" + endPos)
                .add("pkgLength=" + pkgLength)
                .add("remainsBytes=" + remainsBytes)
                .add("packetType=" + packetType)
                .add("proxyBuffer=" + proxyBuffer)
                .toString();
    }

    public MySQLPacketInf(boolean CLIENT_DEPRECATE_EOF, CapabilityFlags capabilityFlags, BufferPool bufferPool) {
        this.CLIENT_DEPRECATE_EOF = CLIENT_DEPRECATE_EOF;
        this.capabilityFlags = capabilityFlags;
        this.bufPool = bufferPool;
        this.proxyBuffer =  new ProxyBuffer(bufferPool.allocate());
        multiPackets.add(proxyBuffer);
    }

    public MySQLPacketInf(BufferPool bufferPool) {
        this(Boolean.FALSE, MySQLSession.getClientCapabilityFlags(), bufferPool);
    }

    /**
     * 使用共享的Buffer
     *
     * @param sharedBuffer
     */
    public void useSharedBuffer(ProxyBuffer sharedBuffer) {
        if (this.proxyBuffer != null && !referedBuffer &&sharedBuffer!=null) {
            recycleAllocedBuffer(proxyBuffer);
            proxyBuffer = sharedBuffer;
            this.referedBuffer = true;
            logger.debug("use sharedBuffer. ");
        } else if (this.proxyBuffer == null) {
            logger.debug("proxyBuffer is null.{}", this);
            throw new RuntimeException("proxyBuffer is null.");
            // proxyBuffer = sharedBuffer;
        } else if (sharedBuffer == null) {
            logger.debug("referedBuffer is false.");
            proxyBuffer = new ProxyBuffer(this.bufPool.allocate());
            proxyBuffer.reset();
            this.referedBuffer = false;
        }else {
            throw new MycatException("useSharedBuffer proxyBuffer:%s  referedBuffer:%s sharedBuffer:%s"
                    ,proxyBuffer.toString(), referedBuffer,sharedBuffer.toString());
        }
    }

    /**
     * 手动创建的ProxyBuffer需要手动释放，recycleAllocedBuffer()
     *
     * @return ProxyBuffer
     */
    public ProxyBuffer allocNewProxyBuffer() {
        return new ProxyBuffer(bufPool.allocate());
    }

    public ProxyBuffer allocNewProxyBuffer(int len) {
        return new ProxyBuffer(bufPool.allocate(len));
    }

    /**
     * 释放手动分配的ProxyBuffer
     *
     * @param curFrontBuffer
     */
    public void recycleAllocedBuffer(ProxyBuffer curFrontBuffer) {
        if (curFrontBuffer != null) {
            this.bufPool.recycle(curFrontBuffer.getBuffer());
            curFrontBuffer.setBuffer(null);
        } else {
            logger.error("curFrontBuffer is null,please fix it !!!!");
        }
    }

    public void ensureFreeSpaceOfReadBuffer() {
        int pkgLength = this.pkgLength;
        ByteBuffer buffer = proxyBuffer.getBuffer();
        ProxyConfig config = ProxyRuntime.INSTANCE.getConfig().getConfig(ConfigEnum.PROXY);
        // need a large buffer to hold the package
        if (pkgLength > config.getProxy().getMax_allowed_packet()) {
            throw new IllegalArgumentException("Packet size over the limit.");
        } else if (buffer.capacity() < pkgLength) {
            logger.debug("need a large buffer to hold the package.{}", this);
            lastLargeMessageTime = TimeUtil.currentTimeMillis();
            simpleAdjustCapacityProxybuffer(proxyBuffer, proxyBuffer.writeIndex + pkgLength);
        } else {
            if (proxyBuffer.writeIndex > 0) {
                // compact bytebuffer only
                proxyBuffer.compact();
            } else {
                //  throw new RuntimeException(" not enough space");
            }
        }
    }

    /**
     * 重置buffer
     *
     * @param newBuffer
     */
    private void resetBuffer(ByteBuffer newBuffer) {
        newBuffer.put(proxyBuffer.getBytes(proxyBuffer.readIndex, proxyBuffer.writeIndex - proxyBuffer.readIndex));
        recycleAllocedBuffer(proxyBuffer);
        proxyBuffer.resetBuffer(newBuffer);
        this.endPos = this.endPos - this.startPos;
        this.startPos = 0;
    }

    /**
     * lastLargeMessageTime < lastReadTime - 30 * 1000L
     * 检查 是否需要切换回正常大小buffer.
     */
    public void changeToDirectIfNeed(boolean condition) {
        if (!proxyBuffer.getBuffer().isDirect()) {
            if (this.pkgLength > bufPool.getChunkSize()) {
                lastLargeMessageTime = TimeUtil.currentTimeMillis();
                return;
            }
            if (condition) {
                logger.info("change to direct con read buffer ,cur temp buf size : {}",
                        proxyBuffer.getBuffer().capacity());
                ByteBuffer bytebuffer = bufPool.allocate();
                if (!bytebuffer.isDirect()) {
                    bufPool.recycle(bytebuffer);
                } else {
                    resetBuffer(bytebuffer);
                }
            }
        }
    }

    public void checkBufferOwner(boolean bufferReadstate) {
        if (!curBufOwner) {
            throw new java.lang.IllegalArgumentException("buffer not changed to me ");
        } else if (this.proxyBuffer.isInReading() != bufferReadstate) {
            throw new java.lang.IllegalArgumentException(
                    "buffer not in correcte state ,expected state  " + (bufferReadstate ? " readable " : "writable "));
        }
    }


    public boolean isCurBufOwner() {
        return curBufOwner;
    }

    public void setCurBufOwner(boolean curBufOwner) {
        this.curBufOwner = curBufOwner;
    }

    private final static Logger logger = LoggerFactory.getLogger(MySQLPacketInf.class);

    public void writeMySQLPacket(MySQLPacket pkg) {
        this.proxyBuffer.reset();
        pkg.write(this.proxyBuffer);
        proxyBuffer.flip();
        proxyBuffer.readIndex = proxyBuffer.writeIndex;
    }

    public void writeByteArray(byte[] pkg) {
        this.proxyBuffer.reset();
        proxyBuffer.writeBytes(pkg);
        proxyBuffer.flip();
        proxyBuffer.readIndex = proxyBuffer.writeIndex;
    }
    public void setResponse(){
        this.state = ComQueryState.FIRST_PACKET;
    }
    public void reset() {
        head = 0;
        startPos = 0;
        endPos = 0;
        pkgLength = 0;
        remainsBytes = 0;//还有多少字节才结束，仅对跨多个Buffer的MySQL报文有意义（crossBuffer=true)
        crossPacket = false;
        packetType = PacketType.SHORT_HALF;

        proxyBuffer.reset();
        int size = this.multiPackets.size();
        for (int i = 0; i < size; i++) {
            ProxyBuffer proxyBuffer = this.multiPackets.get(i);
            if (proxyBuffer!=null&&proxyBuffer!= this.proxyBuffer) {
                bufPool.recycle(this.proxyBuffer.getBuffer());
            }
        }
        payloadReader.reset();
        multiPacketWriter.reset();
        sqlType = 0;
        prepareFieldNum = 0;
        prepareParamNum = 0;
        columnCount = 0;
        serverStatus = 0;
        packetId = 0;
        this.state = ComQueryState.QUERY_PACKET;
        mysqlPacketType = MySQLPayloadType.UNKNOWN;
        hasResolvePayloadType = false;
    }
}
