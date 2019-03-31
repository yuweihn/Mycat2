package io.mycat.mysql;

import io.mycat.proxy.ProxyBuffer;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * cjw
 * 294712221@qq.com
 */
public class PacketListToPayloadReader implements Iterator<ProxyBuffer> {
    int index = 0;
    final LinkedList<ProxyBuffer> multiPackets;
    ByteBuffer curBytebuffer;
    int length = 0;

    int iteratorIndex = 0;

    public void reset() {
        curBytebuffer = null;
        index = 0;
        length = 0;
        iteratorIndex = 0;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public int getIndex() {
        return index;
    }


    public PacketListToPayloadReader(final LinkedList<ProxyBuffer> multiPackets) {
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
        iteratorIndex = 0;
    }

    private void loadPacket(ProxyBuffer proxyBuffer) {
        curBytebuffer = proxyBuffer.getBuffer();
        curBytebuffer.position(proxyBuffer.readIndex + 4);
        curBytebuffer.limit(proxyBuffer.writeIndex);
        this.curBytebuffer = proxyBuffer.getBuffer();
    }

    public byte[] getBytes() {
        byte[] bytes = new byte[length];
        int i = 0;
        while (i < length) {
            bytes[i] = get();
            ++i;
        }
        return bytes;
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

    public void changeToIterator(){
        curBytebuffer = null;
        index = 0;
        length = 0;
    }

    @Override
    public boolean hasNext() {
        return !multiPackets.isEmpty();
    }

    @Override
    public ProxyBuffer next() {
        return multiPackets.removeFirst();
    }
}
