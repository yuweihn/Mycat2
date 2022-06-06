package io.mycat.monitor;

import io.mycat.IOExecutor;
import io.mycat.MetaClusterCurrent;
import io.mycat.beans.mycat.MycatRelDataType;
import io.mycat.config.ServerConfig;
import io.mycat.config.TimerConfig;
import io.mycat.newquery.MysqlCollector;
import io.mycat.newquery.NewMycatConnection;
import io.mycat.newquery.RowSet;
import io.mycat.newquery.SqlResult;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class ThreadMycatConnectionImplWrapper implements NewMycatConnection {
    private DatabaseInstanceEntry stat;
    final NewMycatConnection newMycatConnection;
    final Long timeId;

    public ThreadMycatConnectionImplWrapper(DatabaseInstanceEntry stat, NewMycatConnection newMycatConnection) {
        this.stat = stat;
        this.newMycatConnection = newMycatConnection;

        Vertx vertx = MetaClusterCurrent.wrapper(Vertx.class);
        ServerConfig serverConfig = MetaClusterCurrent.wrapper(ServerConfig.class);
        TimerConfig idleTimer = serverConfig.getIdleTimer();
        if (idleTimer != null && idleTimer.getPeriod() > 0) {
            long period = TimeUnit.valueOf(idleTimer.getTimeUnit()).toMillis(idleTimer.getPeriod());
            timeId = vertx.setPeriodic(period, id -> {
                if (newMycatConnection.isClosed()) {
                    vertx.cancelTimer(id);
                }else {
                    long duration = System.currentTimeMillis() - newMycatConnection.getActiveTimeStamp();
                    if (duration > period) {
                        ThreadMycatConnectionImplWrapper.this.abandonConnection();
                        vertx.cancelTimer(id);
                    }
                }
            });
        } else {
            timeId = null;
        }
    }

    @Override
    public String getTargetName() {
        return this.newMycatConnection.getTargetName();
    }

    @Override
    public Future<RowSet> query(String sql, List<Object> params) {
        IOExecutor ioExecutor = MetaClusterCurrent.wrapper(IOExecutor.class);
        return ioExecutor.executeBlocking(promise -> {
            try {
                this.stat.plusThread();
                newMycatConnection.query(sql, params).onComplete(promise);
            } catch (Exception e) {
                promise.tryFail(e);
            } finally {
                this.stat.decThread();
            }
        });
    }

    @Override
    public void prepareQuery(String sql, List<Object> params, MysqlCollector collector) {
        IOExecutor ioExecutor = MetaClusterCurrent.wrapper(IOExecutor.class);
        ioExecutor.executeBlocking(promise -> {
            try {
                this.stat.plusThread();
                newMycatConnection.prepareQuery(sql, params, collector);
                promise.tryComplete();
            } catch (Exception e) {
                promise.tryFail(e);
            } finally {
                this.stat.decThread();
            }
        });
    }

    @Override
    public Observable<VectorSchemaRoot> prepareQuery(String sql, List<Object> params, MycatRelDataType mycatRelDataType, BufferAllocator allocator) {
        return newMycatConnection.prepareQuery(sql, params, mycatRelDataType, allocator);
    }

    @Override
    public Observable<VectorSchemaRoot> prepareQuery(String sql, List<Object> params, BufferAllocator allocator) {
        return newMycatConnection.prepareQuery(sql, params, allocator);
    }

    @Override
    public Observable<Buffer> prepareQuery(String sql, List<Object> params, int serverstatus) {
        return newMycatConnection.prepareQuery(sql, params, serverstatus);
    }

    @Override
    public Future<List<Object>> call(String sql) {
        return newMycatConnection.call(sql);
    }

    @Override
    public Future<SqlResult> insert(String sql, List<Object> params) {
        IOExecutor ioExecutor = MetaClusterCurrent.wrapper(IOExecutor.class);
        return ioExecutor.executeBlocking(promise -> {
            try {
                this.stat.plusThread();
                newMycatConnection.insert(sql, params).onComplete(promise);
            } catch (Exception e) {
                promise.tryFail(e);
            } finally {
                this.stat.decThread();
            }
        });
    }

    @Override
    public Future<SqlResult> insert(String sql) {
        IOExecutor ioExecutor = MetaClusterCurrent.wrapper(IOExecutor.class);
        return ioExecutor.executeBlocking(promise -> {
            try {
                this.stat.plusThread();
                newMycatConnection.insert(sql).onComplete(promise);
            } catch (Exception e) {
                promise.tryFail(e);
            } finally {
                this.stat.decThread();
            }
        });
    }

    @Override
    public Future<SqlResult> update(String sql) {
        IOExecutor ioExecutor = MetaClusterCurrent.wrapper(IOExecutor.class);
        return ioExecutor.executeBlocking(promise -> {
            try {
                this.stat.plusThread();
                newMycatConnection.update(sql).onComplete(promise);
            } catch (Exception e) {
                promise.tryFail(e);
            } finally {
                this.stat.decThread();
            }
        });
    }

    @Override
    public Future<SqlResult> update(String sql, List<Object> params) {
        IOExecutor ioExecutor = MetaClusterCurrent.wrapper(IOExecutor.class);
        return ioExecutor.executeBlocking(promise -> {
            try {
                this.stat.plusThread();
                newMycatConnection.update(sql, params).onComplete(promise);
            } catch (Exception e) {
                promise.tryFail(e);
            } finally {
                this.stat.decThread();
            }
        });
    }

    @Override
    public Future<Void> close() {
        IOExecutor ioExecutor = MetaClusterCurrent.wrapper(IOExecutor.class);
        if (timeId != null) {
            Vertx vertx = MetaClusterCurrent.wrapper(Vertx.class);
            vertx.cancelTimer(timeId);
        }
        return ioExecutor.executeBlocking(promise -> {
            try {
                this.stat.plusThread();
                newMycatConnection.close().onComplete(promise);
            } catch (Exception e) {
                promise.tryFail(e);
            } finally {
                this.stat.decThread();
            }
        });
    }

    @Override
    public boolean isClosed() {
        return newMycatConnection.isClosed();
    }

    @Override
    public void abandonConnection() {
        if (timeId != null) {
            Vertx vertx = MetaClusterCurrent.wrapper(Vertx.class);
            vertx.cancelTimer(timeId);
        }
        IOExecutor ioExecutor = MetaClusterCurrent.wrapper(IOExecutor.class);
        ioExecutor.executeBlocking(promise -> {
            try {
                this.stat.plusThread();
                newMycatConnection.abandonConnection();
                promise.tryComplete();
            } catch (Exception e) {
                promise.tryFail(e);
            } finally {
                this.stat.decThread();
            }
        });
    }

    @Override
    public Future<Void> abandonQuery() {
        IOExecutor ioExecutor = MetaClusterCurrent.wrapper(IOExecutor.class);
        return ioExecutor.executeBlocking(promise -> {
            try {
                this.stat.plusThread();
                newMycatConnection.abandonQuery().onComplete(promise);
            } catch (Exception e) {
                promise.tryFail(e);
            } finally {
                this.stat.decThread();
            }
        });
    }

    @Override
    public boolean isQuerying() {
        return this.newMycatConnection.isQuerying();
    }

    @Override
    public void onActiveTimestamp(long timestamp) {
        this.newMycatConnection.onActiveTimestamp(timestamp);
    }

    @Override
    public long getActiveTimeStamp() {
        return this.newMycatConnection.getActiveTimeStamp();
    }
}
