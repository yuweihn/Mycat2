package io.mycat.lib;

import cn.lightfish.pattern.InstructionSet;
import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.lib.impl.CacheLib;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

public class CacheResultSetExport implements InstructionSet {

    public static MycatResultSetResponse cacheResponse(String key, Supplier<MycatResultSetResponse> supplier) {
        return CacheLib.cacheResponse(key,supplier);
    }

    public static void removeCache(String key) {
         CacheLib.removeCache(key);
    }
}