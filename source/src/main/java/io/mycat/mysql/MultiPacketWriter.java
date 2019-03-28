package io.mycat.mysql;

import io.mycat.proxy.ProxyBuffer;

import java.util.Iterator;
import java.util.List;

public class MultiPacketWriter implements Iterator<ProxyBuffer> {
    private final List<ProxyBuffer> byteBuffers;
    int index = 0;

    public MultiPacketWriter(List<ProxyBuffer> byteBuffers) {
        this.byteBuffers = byteBuffers;
    }


    public boolean hasNext() {
        return byteBuffers.size()>index;
    }

    public void reset(){
        index = 0;
    }

    public ProxyBuffer next() {
        int index = this.index;
        this.index++;
        ProxyBuffer remove = byteBuffers.get(index);
        remove.readIndex = remove.writeIndex;
        remove.flip();
        return remove;
    }
}
