package io.mycat.util;

import io.mycat.ExplainDetail;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.RowIterable;

import java.util.function.Supplier;

public interface Response {

    void sendError(Throwable e);

    void proxySelect(String defaultTargetName, String statement);

    void proxyUpdate(String defaultTargetName, String proxyUpdate);

    void tryBroadcastShow(String statement);

    void sendError(String errorMessage, int errorCode);

    void sendResultSet(RowIterable rowIterable);

    default void sendResultSet(Supplier<RowBaseIterator> rowBaseIteratorSupplier) {
        sendResultSet(rowBaseIteratorSupplier.get());
    }

    default void sendResultSet(RowBaseIterator rowBaseIterator) {
        sendResultSet(RowIterable.create(rowBaseIterator));
    }

    void rollback();

    void begin();

    void commit();

    void execute(ExplainDetail detail);

    default void sendOk() {
        sendOk(0, 0);
    }

    void sendOk(long lastInsertId, long affectedRow);

    <T> T unWrapper(Class<T> clazz);
}