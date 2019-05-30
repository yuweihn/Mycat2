package io.mycat.mysql;

import io.mycat.proxy.ProxyBuffer;

import java.util.Iterator;
import java.util.List;

public class MultiPacketWriter implements Iterator<ProxyBuffer> {
    private Iterator<ProxyBuffer> byteBuffers;

    public void init(Iterator<ProxyBuffer> byteBuffers) {
        this.byteBuffers = byteBuffers;
    }

    public boolean hasNext() {
        boolean b = byteBuffers.hasNext();
        if (!b){
            byteBuffers = null;
        }
        return b;
    }

    public void reset(){
        byteBuffers = null;
    }

    public ProxyBuffer next() {
        ProxyBuffer remove = byteBuffers.next();
        remove.readIndex = remove.writeIndex;
        remove.flipToReading();
        return remove;
    }
}
