/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.vertx;

import io.mycat.beans.mysql.packet.MySQLPacketSplitter;
import io.mycat.mycatmysql.MycatVertxMySQLHandler;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;

import static io.mycat.vertx.VertxMySQLPacketResolver.State.HEAD;
import static io.mycat.vertx.VertxMySQLPacketResolver.State.PAYLOAD;

public class VertxMySQLPacketResolver implements Handler<Buffer> {
    Buffer head;
    Buffer payload;
    State state = HEAD;
    int currentPacketLength;
    int reveicePacketLength = 0;
    MycatVertxMySQLHandler mySQLHandler;
    NetSocket socket;
    private short packetId;

    public static enum State {
        HEAD,
        PAYLOAD
    }


    public VertxMySQLPacketResolver(NetSocket socket, MycatVertxMySQLHandler mySQLHandler) {
        this.mySQLHandler = mySQLHandler;
        this.socket = socket;
    }

    @Override
    public void handle(Buffer event) {
        for (; ; ) {
            if (event.length()==0)return;
            switch (state) {
                case HEAD: {
                    if (head == null) {
                        head = Buffer.buffer();
                    }
                    int restEnd = 4 - head.length();
                    head.appendBuffer(event.slice(0, Math.min(event.length(), restEnd)));
                    if (head.length() < 4) {
                        state = HEAD;
                        return;
                    } else {
                        state = PAYLOAD;
                        event = event.slice(restEnd, event.length());
                    }
                    if (state == PAYLOAD) {
                        currentPacketLength = readInt(head, 0, 3);
                        reveicePacketLength = 0;
                        this.packetId = head.getUnsignedByte(3);
                        head = null;//help gc
                    }
                    continue;
                }
                case PAYLOAD: {
                    if (payload == null) {
                        payload = Buffer.buffer();
                    }
                    int restPacketLength = currentPacketLength - reveicePacketLength;
                    Buffer curBuffer;
                    if (event.length() > restPacketLength) {
                        curBuffer = event.slice(0, restPacketLength);
                        event = event.slice(restPacketLength, event.length());
                    } else {
                        curBuffer = event;
                        event =Buffer.buffer();
                    }
                    payload.appendBuffer(curBuffer);
                    reveicePacketLength += curBuffer.length();
                    boolean endPacket = currentPacketLength == reveicePacketLength;
                    boolean multiPacket = (currentPacketLength == MySQLPacketSplitter.MAX_PACKET_SIZE);
                    if (endPacket){
                        state = HEAD;
                        this.head = null;
                        reveicePacketLength = 0;
                    }
                    if (endPacket && !multiPacket) {
                        Buffer payload = this.payload;
                        this.payload = null;
                        mySQLHandler.handle(packetId, payload, socket);
                        continue;
                    }else {
                        continue;
                }
            }
            default:
                throw new IllegalStateException("Unexpected value: " + state);
        }
    }

}

    public static int readInt(Buffer buffer, int start, int length) {
        int rv = 0;
        for (int i = 0; i < length; i++) {
            byte b = buffer.getByte(start + i);
            rv |= (((long) b) & 0xFF) << (i * 8);
        }
        return rv;
    }
}
