package io.mycat.mycat2.bufferTest;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.cmds.judge.JudgeUtil;
import io.mycat.mysql.CapabilityFlags;
import io.mycat.mysql.packet.EOFPacket;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.mysql.packet.OKPacket;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.util.ParseUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.StringJoiner;

import static io.mycat.mycat2.bufferTest.MySQLRespPacketType.EOF;
import static io.mycat.util.ParseUtil.msyql_packetHeaderSize;
import static io.mycat.util.ParseUtil.mysql_packetTypeSize;

public class MySQLProxyPacketResolver {
    protected static Logger logger = LoggerFactory.getLogger(MySQLProxyPacketResolver.class);
    int sqlType = -1;
    int prepareFieldNum = 0;
    int prepareParamNum = 0;
    long columnCount = 0;
    int serverStatus = 0;


    int lastPacketId = 0;
    ComQueryRespState state = ComQueryRespState.FIRST_PACKET;
//    boolean willBeFinished = false;

    public MySQLRespPacketType mysqlPacketType = MySQLRespPacketType.UNKNOWN;
    boolean crossPacket = false;

    public void updatePayloadState(
            int sqlType,
            int prepareFieldNum,
            int prepareParamNum,
            long columnCount,
            int serverStatus,
            int lastPacketId,
            ComQueryRespState state,
            MySQLRespPacketType mysqlPacketType,
            boolean payloadFinishedInThisPacket
    ) {
        this.sqlType = sqlType;
        this.prepareFieldNum = prepareFieldNum;
        this.prepareParamNum = prepareParamNum;
        this.columnCount = columnCount;
        this.serverStatus = serverStatus;
        this.lastPacketId = lastPacketId;
        this.state = state;
        this.mysqlPacketType = mysqlPacketType;
        this.crossPacket = payloadFinishedInThisPacket;
    }


    CapabilityFlags capabilityFlags;

    public MySQLProxyPacketResolver() {
        this(MySQLSession.getClientCapabilityFlags(), Boolean.FALSE);
    }

    public MySQLProxyPacketResolver(CapabilityFlags capabilityFlags, boolean CLIENT_DEPRECATE_EOF) {
        this.capabilityFlags = capabilityFlags;
        this.CLIENT_DEPRECATE_EOF = CLIENT_DEPRECATE_EOF;
    }

    boolean CLIENT_DEPRECATE_EOF;


    public static final int LONG_HALF_MIN_LENGTH = msyql_packetHeaderSize + mysql_packetTypeSize;
    public static final int RESULT_MIN_LENGTH = 13;


    public PayloadType resolveFullPayload(PacketInf packetInf) {
        PacketType type = resolveMySQLPackage(packetInf);
        if (type == PacketType.FULL) {//终止条件
            return !this.crossPacket ? PayloadType.FULL_PAYLOAD : PayloadType.TYPE_PAYLOAD;
        } else if (type == PacketType.LONG_HALF) {
            return PayloadType.TYPE_PAYLOAD;
        }
        return PayloadType.HALF_PAYLOAD;
    }

    public PayloadType resolveCrossBufferFullPayload(PacketInf packetInf) {
        boolean crossPacket = this.crossPacket;
        PacketType type = resolveMySQLPackage(packetInf);
        if (!this.crossPacket && (type == PacketType.FINISHED_CROSS || type == PacketType.FULL)) {
            return PayloadType.FINISHED_CROSS_PAYLOAD;
        } else if (type == PacketType.LONG_HALF) {
            return packetInf.crossBuffer() || crossPacket ? PayloadType.REST_CROSS_PAYLOAD : PayloadType.HALF_PAYLOAD;
        } else if (type == PacketType.SHORT_HALF) {
            return PayloadType.HALF_PAYLOAD;
        } else if (type == PacketType.REST_CROSS) {
            return PayloadType.REST_CROSS_PAYLOAD;
        } else if (type == PacketType.FULL) {
            return PayloadType.REST_CROSS_PAYLOAD;
        }
        throw new RuntimeException("unknown state!");
    }

    public PacketType resolveMySQLPackage(PacketInf packetInf) {
        int offset = packetInf.proxyBuffer.readIndex;   // 读取的偏移位置
        int limit = packetInf.proxyBuffer.writeIndex;   // 读取的总长度
        int totalLen = limit - offset;      // 读取当前的总长度
        ByteBuffer buffer = packetInf.proxyBuffer.getBuffer();
        switch (packetInf.packetType) {
            case SHORT_HALF:
            case FULL:
            case FINISHED_CROSS: {
                if (totalLen > 3) {//totalLen >= 4
                    int packetId = buffer.get(offset + 3) & 0xff;
                    checkPacketId(packetId);
                    packetInf.packetId = (packetId);
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
                        return resolveCrossPacket(packetInf, offset, limit, totalLen);
                    }
                    if (totalLen > 4) {
                        return resolveShort2FullOrLongHalf(packetInf, offset, limit, buffer, isPacketFinished);
                    }
                    if (totalLen == 4 && packetInf.pkgLength == 4) {
                        this.updatePayloadState(this.sqlType, this.prepareFieldNum, this.prepareParamNum, this.columnCount, this.serverStatus, this.lastPacketId,
                                this.state, MySQLRespPacketType.UNKNOWN, false);
                        packetInf.endPos = 4;
                        return packetInf.packetType = PacketType.FULL;
                    }

                }
                this.updatePayloadState(this.sqlType, this.prepareFieldNum, this.prepareParamNum, this.columnCount, this.serverStatus, this.lastPacketId,
                        this.state, MySQLRespPacketType.UNKNOWN, false);
                return packetInf.change2ShortHalf();
            }
            case LONG_HALF:
                return resolveLongHalf(packetInf, offset, limit, totalLen, buffer);
            case REST_CROSS:
                return resolveRestCross(packetInf, offset, limit, totalLen);
            default:
                throw new RuntimeException("unknown state!");

        }
    }

    private PacketType resolveShort2FullOrLongHalf(PacketInf packetInf, int offset, int limit, ByteBuffer buffer, boolean isPacketFinished) {
        packetInf.head = buffer.get(offset + 4) & 0xff;
        judgeMetaDataPacket(packetInf);
        if (!packetInf.isMetaData) {
            resolvePayloadType(packetInf, isPacketFinished);
        }
        packetInf.startPos = offset;
        if (isPacketFinished) {
            packetInf.endPos = offset + packetInf.pkgLength;
            if (packetInf.isMetaData && !this.crossPacket) {
                resolvePayloadType(packetInf, true);
            }
            return packetInf.packetType = PacketType.FULL;
        } else {
            packetInf.endPos = limit;
            return packetInf.packetType = PacketType.LONG_HALF;
        }
    }

    private void judgeMetaDataPacket(PacketInf packetInf) {
        switch (state) {
            case COLUMN_DEFINITION:
                packetInf.isMetaData = false;
                break;
            case PREPARE_RESPONSE:
            case FIRST_PACKET:
            case COLUMN_END_EOF:
                packetInf.isMetaData = true;
                break;
            case RESULTSET_ROW:
                packetInf.isMetaData = (packetInf.head == 0xfe) && packetInf.pkgLength < 0xffffff;
                break;
            default:
                throw new RuntimeException("unknown state!");
        }
    }

    private void checkPacketId(int packetId) {
        if (++lastPacketId != packetId) {
            throw new RuntimeException("packetId should be " + lastPacketId + " that is not match " + packetId);
        }
    }

    private PacketType resolveCrossPacket(PacketInf packetInf, int offset, int limit, int totalLen) {
        packetInf.startPos = offset;
        if (totalLen >= packetInf.pkgLength) {
            packetInf.endPos = offset + packetInf.pkgLength;
            if (packetInf.isMetaData) {
                resolvePayloadType(packetInf, true);
            }
            return packetInf.packetType = PacketType.FULL;
        } else {
            packetInf.endPos = limit;
            return packetInf.packetType = PacketType.LONG_HALF;
        }
    }

    public void resolvePayloadType(PacketInf packetInf, boolean isPacketFinished) {
        int head = packetInf.head;
        switch (state) {
            case FIRST_PACKET: {
                if (!isPacketFinished) throw new RuntimeException("unknown state!");
                if (head == 0xff) {
                    this.mysqlPacketType = MySQLRespPacketType.ERROR;
                    state = ComQueryRespState.END;
                } else if (head == 0x00) {
                    if (sqlType == MySQLCommand.COM_STMT_PREPARE && packetInf.packetId == 1 && packetInf.pkgLength == 16) {
                        state = ComQueryRespState.PREPARE_RESPONSE;
                        ProxyBuffer buffer = packetInf.proxyBuffer;
                        buffer.readIndex = packetInf.startPos + 9;
                        this.prepareFieldNum = (int) buffer.readFixInt(2);
                        this.prepareParamNum = (int) buffer.readFixInt(2);
                        this.mysqlPacketType = MySQLRespPacketType.PREPARE_OK;
                    } else {
                        this.mysqlPacketType = MySQLRespPacketType.OK;
                        this.serverStatus = OKPacket.readServerStatus(packetInf.proxyBuffer, capabilityFlags);
                        state = ComQueryRespState.END;
                    }
                } else if (head == 0xfb) {
                    throw new UnsupportedOperationException("unsupport LOCAL INFILE!");
                } else if (head == 0xfe) {
                    this.mysqlPacketType = EOF;
                    this.serverStatus = EOFPacket.readStatus(packetInf.proxyBuffer);
                    state = ComQueryRespState.END;
                } else {
                    ProxyBuffer proxyBuffer = packetInf.proxyBuffer;
                    columnCount = proxyBuffer.getLenencInt(packetInf.startPos + 4);
                    this.mysqlPacketType = MySQLRespPacketType.COULUMN_DEFINITION;
                    state = ComQueryRespState.END;
                }
                return;
            }
            case COLUMN_DEFINITION: {
                --columnCount;
                if (columnCount == 0) {
                    this.state = !this.CLIENT_DEPRECATE_EOF ? ComQueryRespState.COLUMN_END_EOF : ComQueryRespState.RESULTSET_ROW;
                }
                this.mysqlPacketType = MySQLRespPacketType.COULUMN_DEFINITION;
                return;
            }
            case COLUMN_END_EOF: {
                if (!isPacketFinished) throw new RuntimeException("unknown state!");
                this.serverStatus = EOFPacket.readStatus(packetInf.proxyBuffer);
                this.mysqlPacketType = EOF;
                this.state = ComQueryRespState.RESULTSET_ROW;
                return;
            }
            case RESULTSET_ROW: {
                if (head == 0x00) {
                    //binary resultset row
                    this.mysqlPacketType = MySQLRespPacketType.BINARY_RESULTSET_ROW;
                } else if (head == 0xfe && packetInf.pkgLength < 0xffffff) {
                    if (!isPacketFinished) throw new RuntimeException("unknown state!");
                    if (CLIENT_DEPRECATE_EOF) {
                        this.mysqlPacketType = MySQLRespPacketType.OK;
                        //ok
                        serverStatus = OKPacket.readServerStatus(packetInf.proxyBuffer, capabilityFlags);
                    } else {
                        this.mysqlPacketType = EOF;
                        //eof
                        serverStatus = OKPacket.readServerStatus(packetInf.proxyBuffer, capabilityFlags);
                    }
                    if (JudgeUtil.hasMoreResult(serverStatus)) {
                        state = ComQueryRespState.FIRST_PACKET;
                    } else {
                        state = ComQueryRespState.END;
                    }
                } else if (head == MySQLPacket.ERROR_PACKET) {
                    this.mysqlPacketType = MySQLRespPacketType.ERROR;
                    state = ComQueryRespState.END;
                } else {
                    this.mysqlPacketType = MySQLRespPacketType.TEXT_RESULTSET_ROW;
                    //text resultset row
                }
                break;
            }
            default: {
                resolvePrepareResponse(packetInf.proxyBuffer, head, isPacketFinished);
            }
        }
    }

    private PacketType resolveLongHalf(PacketInf packetInf, int offset, int limit, int totalLen, ByteBuffer buffer) {
        packetInf.startPos = offset;
        if (totalLen >= packetInf.pkgLength) {
            packetInf.endPos = offset + packetInf.pkgLength;
            if (packetInf.isMetaData) {
                resolvePayloadType(packetInf, true);
            }
            return packetInf.packetType = PacketType.FULL;
        }
        if (offset == 0 && packetInf.pkgLength > limit && totalLen > buffer.capacity()) {
            packetInf.needExpandBuffer = true;
        }
        packetInf.endPos = limit;
        return packetInf.packetType = PacketType.LONG_HALF;
    }

    private PacketType resolveRestCross(PacketInf packetInf, int offset, int limit, int totalLen) {
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

    private void resolvePrepareResponse(ProxyBuffer proxyBuf, int head, boolean isPacketFinished) {
        if (!isPacketFinished) throw new RuntimeException("unknown state!");
        if (prepareFieldNum > 0) {
            prepareFieldNum--;
            this.mysqlPacketType = MySQLRespPacketType.COULUMN_DEFINITION;
            this.state = ComQueryRespState.PREPARE_FIELD;
            return;
        }
        if (prepareFieldNum == 0 && !CLIENT_DEPRECATE_EOF && ComQueryRespState.PREPARE_FIELD == state&&head == 0xFE) {
            this.serverStatus = EOFPacket.readStatus(proxyBuf);
            this.mysqlPacketType = EOF;
            this.state = ComQueryRespState.PREPARE_PARAM;
            return;
        }
        if (prepareParamNum > 0) {
            prepareParamNum--;
            this.mysqlPacketType = MySQLRespPacketType.COULUMN_DEFINITION;
            this.state = ComQueryRespState.PREPARE_PARAM;
            if (!CLIENT_DEPRECATE_EOF){
                return;
            }else {
                state = ComQueryRespState.END;
                return;
            }
        }
        if (prepareParamNum == 0 && !CLIENT_DEPRECATE_EOF && ComQueryRespState.PREPARE_PARAM == state&&head == 0xFE) {
            this.serverStatus = EOFPacket.readStatus(proxyBuf);
            this.mysqlPacketType = EOF;
            state = ComQueryRespState.END;
            return;
        }
        throw new RuntimeException("unknown state!");
    }

    private void resokveColumnCountPacketInFirstPacket(ProxyBuffer proxyBuf, int offset, int totalLen, int packageLength) {
        int startPos = offset;
        int endPos = offset + packageLength;
        this.columnCount = proxyBuf.getLenencInt(LONG_HALF_MIN_LENGTH);
        this.state = ComQueryRespState.COLUMN_DEFINITION;
    }

    private void resolveOkPacketInFirstPacket(int offset, int totalLen, int packageLength) {
        int startPos = offset;
        int endPos = offset + packageLength;
        if (sqlType == MySQLCommand.COM_STMT_PREPARE) {
            state = ComQueryRespState.PREPARE_RESPONSE;
        }
    }

    private void resolveEOFPacketInFirstPacket(int offset, int totalLen, int packageLength) {

    }


    static enum Direction {
        REQUEST, RESPONSE
    }

    static enum ComQueryRespState {
        FIRST_PACKET,
        COLUMN_DEFINITION,
        PREPARE_RESPONSE,
        COLUMN_END_EOF,
        RESULTSET_ROW,
        PREPARE_FIELD,
        PREPARE_PARAM,
        END
    }

    public static class PacketInf {
        int head;
        int packetId;
        int startPos;
        int endPos;
        int pkgLength;
        int remainsBytes;//还有多少字节才结束，仅对跨多个Buffer的MySQL报文有意义（crossBuffer=true)
        PacketType packetType = PacketType.SHORT_HALF;
        boolean needExpandBuffer;
        ProxyBuffer proxyBuffer;
        boolean isMetaData = false;

        public void updateState(
                int head,
                int packetId,
                int startPos,
                int endPos,
                int pkgLength,
                int remainsBytes,
                PacketType packetType,
                boolean needExpandBuffer,
                ProxyBuffer proxyBuffer,
                boolean isMetaData
        ) {
            String oldState = null;
            if (logger.isDebugEnabled()) {
                oldState = this.toString();
                logger.debug("from {}", oldState);
            }
            this.head = head;
            this.packetId = packetId;
            this.startPos = startPos;
            this.endPos = endPos;
            this.pkgLength = pkgLength;
            this.remainsBytes = remainsBytes;
            this.packetType = packetType;
            this.needExpandBuffer = needExpandBuffer;
            this.proxyBuffer = proxyBuffer;
            this.isMetaData = isMetaData;
            if (logger.isDebugEnabled()) {
                logger.debug("to   {}", this.toString());
            }
        }


        public PacketInf(ProxyBuffer proxyBuffer) {
            this.proxyBuffer = proxyBuffer;
        }

//        public void resetByPacketId(int packetId) {
//            this.updateState(0, packetId, 0, 0, 0, 0, PacketType.SHORT_HALF,
//                    false, proxyBuffer, this.isMetaData);
//        }

        public PacketType change2ShortHalf() {
            this.updateState(0, packetId, 0, 0, 0, 0, PacketType.SHORT_HALF,
                    false, proxyBuffer, false);
            return PacketType.SHORT_HALF;
        }

        public boolean crossBuffer() {
            if (this.packetType == PacketType.LONG_HALF && !isMetaData) {
                if (this.remainsBytes == 0 && !ParseUtil.validateHeader(this.startPos, this.endPos)) {
                    throw new UnsupportedOperationException("");
                }
                this.proxyBuffer.readIndex = this.endPos;
                this.updateState(this.head, this.packetId, this.startPos, this.endPos, this.pkgLength, this.pkgLength - (this.endPos - this.startPos), PacketType.REST_CROSS,
                        this.needExpandBuffer, proxyBuffer, this.isMetaData);
                return true;
            } else {
                return false;
            }
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
            return new StringJoiner(", ", PacketInf.class.getSimpleName() + "[", "]")
                    .add("head=" + head)
                    .add("packetId=" + packetId)
                    .add("startPos=" + startPos)
                    .add("endPos=" + endPos)
                    .add("pkgLength=" + pkgLength)
                    .add("remainsBytes=" + remainsBytes)
                    .add("packetType=" + packetType)
                    .add("needExpandBuffer=" + needExpandBuffer)
                    .add("proxyBuffer=" + proxyBuffer)
                    .add("isMetaData=" + isMetaData)
                    .toString();
        }
    }

    public static enum PacketType {
        FULL, LONG_HALF, SHORT_HALF, REST_CROSS, FINISHED_CROSS
    }

    public static enum PayloadType {
        HALF_PAYLOAD, TYPE_PAYLOAD, FULL_PAYLOAD, REST_CROSS_PAYLOAD, FINISHED_CROSS_PAYLOAD
    }
}
