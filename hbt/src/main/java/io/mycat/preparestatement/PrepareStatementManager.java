package io.mycat.preparestatement;

import org.jboss.netty.util.internal.ConcurrentHashMap;

import java.util.concurrent.atomic.AtomicLong;

public enum PrepareStatementManager {
    INSTANCE;
    final ConcurrentHashMap<Long, Integer> map = new ConcurrentHashMap<>();
    final ConcurrentHashMap<String, Long> sqlToStmt = new ConcurrentHashMap<>();
    final AtomicLong ids = new AtomicLong(0);

    public int getNum(Long stmtId) {
        return map.get(stmtId);
    }

    public long register(String sql, int num) {
        return sqlToStmt.computeIfAbsent(sql, s -> {
            long andIncrement = ids.getAndIncrement();
            map.put(andIncrement, num);
            return andIncrement;
        });
    }
}