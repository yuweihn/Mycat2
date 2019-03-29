package io.mycat.mysql;

import io.mycat.proxy.ProxyBuffer;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * cjw
 * 294712221@qq.com
 */
public class PacketListToPayloadReader {
    int index = 0;
    final List<ProxyBuffer> multiPackets;
    ByteBuffer curBytebuffer;
    int length;

    public void reset() {
        curBytebuffer = null;
        index = 0;
        length = 0;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public int getIndex() {
        return index;
    }


    public PacketListToPayloadReader(final List<ProxyBuffer> multiPackets) {
        this.multiPackets = multiPackets;
    }

    public void addBuffer(ProxyBuffer buffer) {
        multiPackets.add(buffer);
        length += buffer.writeIndex - buffer.readIndex - 4;
    }

    public void loadFirstPacket() {
        ProxyBuffer proxyBuffer = multiPackets.get(0);
        length += proxyBuffer.writeIndex - proxyBuffer.readIndex - 4;
        loadPacket(proxyBuffer);
    }

    private void loadPacket(ProxyBuffer proxyBuffer) {
        curBytebuffer = proxyBuffer.getBuffer();
        curBytebuffer.position(proxyBuffer.readIndex + 4);
        curBytebuffer.limit(proxyBuffer.writeIndex);
        this.curBytebuffer = proxyBuffer.getBuffer();
    }


    public byte get() {
        if (curBytebuffer.hasRemaining()) {
            return curBytebuffer.get();
        } else {
            ++this.index;
            if (this.index < multiPackets.size()) {
                loadPacket(multiPackets.get(this.index));
                return get();
            } else {
                throw new RuntimeException("out of size");
            }
        }
    }

    public int length() {
        return length;
    }
}
