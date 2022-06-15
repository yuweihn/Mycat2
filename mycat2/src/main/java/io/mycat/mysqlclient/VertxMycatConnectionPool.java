/**
 * Copyright (C) <2022>  <chen junwen>
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
package io.mycat.mysqlclient;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLReplaceable;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.google.common.collect.ImmutableList;
import io.mycat.PreparedStatement;
import io.mycat.beans.mycat.MycatMySQLRowMetaData;
import io.mycat.beans.mycat.MycatRelDataType;
import io.mycat.beans.mysql.packet.ColumnDefPacket;
import io.mycat.commands.JdbcDatasourcePoolImpl;
import io.mycat.mysqlclient.decoder.ObjectArrayDecoder;
import io.mycat.newquery.MysqlCollector;
import io.mycat.newquery.NewMycatConnection;
import io.mycat.newquery.RowSet;
import io.mycat.newquery.SqlResult;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public class VertxMycatConnectionPool implements NewMycatConnection {
    private String targetName;
    VertxConnection connection;
    private VertxPoolConnectionImpl vertxConnectionPool;
    Future<Void> queryCloseFuture = Future.succeededFuture();

    private boolean close = false;

    private long activeTimeStamp;

    public VertxMycatConnectionPool(String targetName, VertxConnection connection, VertxPoolConnectionImpl vertxConnectionPool) {
        this.targetName = targetName;
        this.connection = connection;
        this.vertxConnectionPool = vertxConnectionPool;
    }

    @Override
    public String getTargetName() {
        return targetName;
    }

    @Override
    public synchronized Future<RowSet> query(String sql, List<Object> params) {
        if (queryCloseFuture.isComplete()) {
            queryCloseFuture = Future.succeededFuture();
        }
        String psql = deparameterize(sql, params);
        Future<RowSet> rowSetFuture = queryCloseFuture.transform(unused -> {
            Promise<RowSet> promise = Promise.promise();
            ObjectArrayDecoder objectArrayDecoder = new ObjectArrayDecoder();
            Observable<Object[]> query = connection.query(psql, objectArrayDecoder);
            onSend();
            Single<RowSet> map = query.subscribeOn(Schedulers.computation()).toList().map(objects -> {

                MycatMySQLRowMetaData mycatMySQLRowMetaData = new MycatMySQLRowMetaData(Arrays.asList(objectArrayDecoder.getColumnDefPackets()));
                return new RowSet(mycatMySQLRowMetaData, objects);
            });
            map = map.doOnSuccess(objects -> promise.tryComplete(objects));
            map = map.doOnError(objects -> promise.tryFail(objects));
            map = map.doOnTerminate(() -> onRev());
            map.subscribe();
            return promise.future();
        });
        this.queryCloseFuture = rowSetFuture.mapEmpty();
        return rowSetFuture;
    }

    public static String deparameterize(String sql, List<Object> params) {
        if (sql.startsWith("be")) {
            return sql;
        }
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);
        sqlStatement.accept(new MySqlASTVisitorAdapter() {
            int index;

            @Override
            public void endVisit(SQLVariantRefExpr x) {
                if ("?".equalsIgnoreCase(x.getName())) {
                    if (index < params.size()) {
                        Object value = params.get(index++);
                        SQLReplaceable parent = (SQLReplaceable) x.getParent();
                        parent.replace(x, PreparedStatement.fromJavaObject(value));
                    }
                }
                super.endVisit(x);
            }
        });
        sql = sqlStatement.toString();
        return sql;
    }

    @Override
    public synchronized void prepareQuery(String sqlArg, List<Object> params, MysqlCollector collector) {
        if (queryCloseFuture.isComplete()) {
            queryCloseFuture = Future.succeededFuture();
        }
        this.queryCloseFuture = this.queryCloseFuture.transform(event -> {
            String sql = deparameterize(sqlArg, params);
            ObjectArrayDecoder objectArrayDecoder = new ObjectArrayDecoder() {
                @Override
                public void onColumnEnd() {
                    super.onColumnEnd();
                    ColumnDefPacket[] columnDefPackets = super.columnDefPackets;
                    collector.onColumnDef(new MycatMySQLRowMetaData(ImmutableList.copyOf(columnDefPackets)));
                }
            };
            Promise<Void> promise = Promise.promise();
            onSend();
            Observable<Object[]> query = connection.query(sql, objectArrayDecoder);
            query.subscribe(objects -> collector.onRow(objects),
                    throwable -> {
                        promise.tryFail(throwable);
                        collector.onError(throwable);
                    },
                    () -> {
                        onRev();
                        promise.tryComplete();
                        collector.onComplete();
                    });
            return promise.future();
        });
    }

    @Override
    public Observable<VectorSchemaRoot> prepareQuery(String sql, List<Object> params, MycatRelDataType mycatRelDataType, BufferAllocator allocator) {
        return Observable.create(emitter -> {
            synchronized (VertxMycatConnectionPool.this) {
                if (queryCloseFuture.isComplete()) {
                    queryCloseFuture = Future.succeededFuture();
                }
                VertxMycatConnectionPool.this.queryCloseFuture = VertxMycatConnectionPool.this.queryCloseFuture
                        .transform(voidAsyncResult -> {
                            return Future.future((Handler<Promise<Observable<Buffer>>>) promise -> {
                                JdbcDatasourcePoolImpl jdbcDatasourcePool = new JdbcDatasourcePoolImpl(targetName);
                                Future<NewMycatConnection> mycatConnectionFuture = jdbcDatasourcePool.getConnection();
                                mycatConnectionFuture.onSuccess(connection -> {
                                            onSend();
                                            Observable<VectorSchemaRoot> observable = connection.prepareQuery(sql, params, mycatRelDataType, allocator);
                                            onRev();
                                            observable = observable.doOnComplete(() -> connection.close());
                                            observable.subscribe(vectorSchemaRoot -> emitter.onNext(vectorSchemaRoot),
                                                    throwable -> emitter.onError(throwable),
                                                    () -> emitter.onComplete());
                                        }).onFailure(event -> emitter.onError(event))
                                        .onComplete(event -> promise.tryComplete());
                            });
                        }).mapEmpty();
            }
        });
    }

    @Override
    public Observable<VectorSchemaRoot> prepareQuery(String sql, List<Object> params, BufferAllocator allocator) {
        return prepareQuery(sql, params, null, allocator);
    }

    @Override
    public Observable<Buffer> prepareQuery(String sql, List<Object> params, int serverstatus) {
        return Observable.create(emitter -> {
            synchronized (VertxMycatConnectionPool.this) {
                if (queryCloseFuture.isComplete()) {
                    queryCloseFuture = Future.succeededFuture();
                }
                VertxMycatConnectionPool.this.queryCloseFuture = VertxMycatConnectionPool.this.queryCloseFuture
                        .transform(voidAsyncResult -> {
                            return Future.future((Handler<Promise<Observable<Buffer>>>) promise -> {
                                Observable<Buffer> observable = connection.query(deparameterize(sql, params));
                                observable.subscribe(buffer -> emitter.onNext(buffer),
                                        throwable -> emitter.onError(throwable),
                                        () -> {
                                            promise.tryComplete();
                                            emitter.onComplete();
                                        });
                            });
                        }).mapEmpty();
            }
        });
    }

    @Override
    public Future<List<Object>> call(String sql) {
        synchronized (this) {
            if (queryCloseFuture.isComplete()) {
                queryCloseFuture = Future.succeededFuture();
            }
            Future<List<Object>> future = this.queryCloseFuture.transform(voidAsyncResult -> {
                JdbcDatasourcePoolImpl jdbcDatasourcePool = new JdbcDatasourcePoolImpl(targetName);
                return jdbcDatasourcePool.getConnection().flatMap(connection -> {
                    onSend();
                    Future<List<Object>> call = connection.call(sql);
                    return call.onComplete(event -> {
                        onRev();
                        connection.close();
                    });
                });
            });
            this.queryCloseFuture = future.mapEmpty();
            return future;
        }
    }

    @Override
    public Future<SqlResult> insert(String sql, List<Object> params) {
        return update(sql, params);
    }

    @Override
    public Future<SqlResult> insert(String sql) {
        return update(sql);
    }

    @Override
    public Future<SqlResult> update(String sql) {
        return Future.future(promise -> {
            synchronized (VertxMycatConnectionPool.this) {
                if (VertxMycatConnectionPool.this.queryCloseFuture.isComplete()) {
                    VertxMycatConnectionPool.this.queryCloseFuture = Future.succeededFuture();
                }
                VertxMycatConnectionPool.this.queryCloseFuture = VertxMycatConnectionPool.this.queryCloseFuture.transform(new Function<AsyncResult<Void>, Future<Void>>() {
                    @Override
                    public Future<Void> apply(AsyncResult<Void> voidAsyncResult) {
                        onSend();
                        return connection.update(sql)
                                .onComplete(event -> promise.handle(event))
                                .onComplete(event -> onRev()).mapEmpty();
                    }
                });
            }
        });
    }

    @Override
    public Future<SqlResult> update(String sql, List<Object> params) {
        return update(deparameterize(sql, params)).onComplete(event -> onRev());
    }

    @Override
    public Future<Void> close() {
        close = true;
        return queryCloseFuture
                .onSuccess(event -> vertxConnectionPool.recycle(connection))
                .onFailure(event -> vertxConnectionPool.kill(connection));
    }

    @Override
    public boolean isClosed() {
        return close;
    }

    @Override
    public void abandonConnection() {
        close = true;
        vertxConnectionPool.kill(connection);
    }

    @Override
    public synchronized Future<Void> abandonQuery() {
        Future<Void> queryCloseFuture = this.queryCloseFuture;
        if (queryCloseFuture == null) {
            return Future.succeededFuture();
        }
        return queryCloseFuture;
    }

    @Override
    public boolean isQuerying() {
        return !this.queryCloseFuture.isComplete();
    }

    @Override
    public void onActiveTimestamp(long timestamp) {
        this.activeTimeStamp = timestamp;
    }

    @Override
    public long getActiveTimeStamp() {
        return this.activeTimeStamp;
    }

    @Override
    public int getRemoveAbandonedTimeoutSecond() {
        return this.connection.getConfig().getRemoveAbandonedTimeoutSecond();
    }
}
