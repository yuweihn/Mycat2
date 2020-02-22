package io.mycat.router.hashFunction;

public class PureJavaCrc32HashFunction implements HashFunction {
    private final io.mycat.router.function.PureJavaCrc32 crc32;

    public PureJavaCrc32HashFunction() {
        this.crc32 = new io.mycat.router.function.PureJavaCrc32();
    }

    @Override
    public long hash(byte[] bytes) {
        crc32.reset();
        crc32.update(bytes, 0, bytes.length);
        return crc32.getValue();
    }
}