/**
 * Copyright [2021] [chen junwen]
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.mycat.vertx.xa.impl;

import cn.mycat.vertx.xa.*;
import com.alibaba.druid.util.JdbcUtils;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.newquery.NewMycatConnection;
import io.mycat.newquery.SqlResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;


public class BaseXaSqlConnection extends AbstractXaSqlConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(BaseXaSqlConnection.class);
    protected final ConcurrentHashMap<String, NewMycatConnection> map = new ConcurrentHashMap<>();
    protected final Map<NewMycatConnection, State> connectionState = Collections.synchronizedMap(new IdentityHashMap<>());
    protected final List<NewMycatConnection> extraConnections = new CopyOnWriteArrayList<>();
    protected final List<Future<Void>> closeList = new CopyOnWriteArrayList<>();
    private final Supplier<MySQLManager> mySQLManagerSupplier;
    protected volatile String xid;


    public BaseXaSqlConnection(MySQLIsolation isolation, Supplier<MySQLManager> mySQLManagerSupplier, XaLog xaLog) {
        super(isolation,xaLog);
        this.mySQLManagerSupplier = mySQLManagerSupplier;
    }

    protected MySQLManager mySQLManager() {
        return mySQLManagerSupplier.get();
    }


    private String getDatasourceName(NewMycatConnection connection) {
        return map.entrySet().stream().filter(p -> p.getValue() == connection).map(e -> e.getKey())
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("unknown connection " + connection));
    }

    /**
     * 1.not allow Nested transaction,double begin.
     * 2.alloc unique xid
     */
    @Override
    public Future<Void> begin() {
        if (inTranscation) {
            LOGGER.warn("xa transaction occur nested transaction,xid:" + xid);
            return Future.succeededFuture();
        }
        inTranscation = true;
        xid = log.nextXid();
        log.beginXa(xid);
        return Future.succeededFuture();
    }


    public Future<NewMycatConnection> getConnection(String targetName) {
//        for (Map.Entry<String, SqlConnection> stringSqlConnectionEntry : map.entrySet()) {
//            AbstractMySqlConnectionImpl value = (AbstractMySqlConnectionImpl) stringSqlConnectionEntry.getValue();
//            value
//        }

        MySQLManager mySQLManager = mySQLManager();
        if (inTranscation) {
            if (map.containsKey(targetName)) {
                return Future.succeededFuture(map.get(targetName));
            } else {
                Future<NewMycatConnection> sqlConnectionFuture = mySQLManager.getConnection(targetName);
                return sqlConnectionFuture.compose(connection -> {
                    map.put(targetName, connection);
                    changeTo(connection, State.XA_INITED);
                    Future<SqlResult> execute = connection.update(String.format(XA_START, xid));
                    return execute.map(r -> changeTo(connection, State.XA_STARTED));
                });
            }
        }
        Future<NewMycatConnection> connection = mySQLManager.getConnection(targetName);
        return connection.map(connection1 -> {
            if (!map.containsKey(targetName)) {
                map.put(targetName, connection1);
            } else {
                extraConnections.add(connection1);
            }
            return connection1;
        });
    }

    @Override
    public List<NewMycatConnection> getExistedTranscationConnections() {
        return new ArrayList<>(map.values());
    }

    /**
     * <p>
     * XA_START to XA_END to XA_ROLLBACK
     * XA_ENDED to XA_ROLLBACK
     * XA_PREPARED to XA_ROLLBACK
     * <p>
     * client blocks until rollback successfully
     */
    @Override
    public Future<Void> rollback() {
        return Future.future((Promise<Void> promise) -> {
            logParticipants();
            Function<NewMycatConnection, Future<Void>> function = c -> {
                Future<Void> future = Future.succeededFuture();
                if (connectionState.isEmpty()) {
                    return Future.succeededFuture();
                }
                State state = connectionState.get(c);
                if (state == null) {
                    return Future.succeededFuture();
                }
                switch (state) {
                    case XA_INITED:
                        return future;
                    case XA_STARTED:
                        future = future.flatMap(unused -> {
                            return c.update(String.format(XA_END, xid))
                                    .map(u -> changeTo(c, State.XA_ENDED)).mapEmpty();
                        });
                    case XA_ENDED:
                    case XA_PREPARED:
                        future = future.flatMap(unuse -> c.update(String.format(XA_ROLLBACK, xid))
                                .map(i -> changeTo(c, State.XA_ROLLBACKED))).mapEmpty();
                }
                return future;
            };
            Future<Void> future = executeTranscationConnection(function);
            future.onComplete(event -> {
//                Throwable cause = event.cause();
//                if (cause instanceof SQLException) {
//                    SQLException sqlException = (SQLException) cause;
//                    if (sqlException.getErrorCode() == 1397 && "XAER_NOTA: Unknown XID".equalsIgnoreCase(sqlException.getMessage())) {
//                        event = Future.succeededFuture();
//                    }
//                }
                log.logRollback(xid, event.succeeded());
                if (event.succeeded()) {
                    inTranscation = false;
                    clearConnections().onComplete(promise);
                } else {
                    Set<String> targets = new HashSet<>(map.keySet());
                    Future<Void> killFuture = kill();
                    killFuture.flatMap((Function<Void, Future<Void>>) unused -> {
                        try {
                            if (tryRecovery(targets)) {
                                return Future.succeededFuture();
                            }
                            String message = "xid:" + getXid() + " recovery fail";
                            LOGGER.info(message);
                            LOGGER.error(message);
                            //@todo 注册调度中心,定时恢复
                            return Future.failedFuture(message);
                        } catch (Exception e) {
                            LOGGER.error("", e);
                            return Future.failedFuture(e);
                        }
                    }).onComplete(promise);
                }
            });
        });
    }

    private boolean tryRecovery(Set<String> targets) throws InterruptedException {
        for (int tryCount = 0; tryCount < 3; tryCount++) {
            HashMap<String, Connection> map = new HashMap<>();
            try {
                for (String target : targets) {
                    Connection writeableConnection = mySQLManager().getWriteableConnection(target);
                    map.put(target, writeableConnection);
                }
                log.readXARecoveryLog(map);
                return true;
            } catch (Exception e) {
                LOGGER.error("", e);
            } finally {
                map.values().forEach(c -> {
                    if (c != null) {
                        JdbcUtils.close(c);
                    }
                });
            }
            TimeUnit.SECONDS.sleep(1);
        }
        return false;
    }

    /**
     * retry has a delay time for datasource need duration to recover
     */
    private Future<Void> retryRollback(Function<NewMycatConnection, Future<Void>> function) {
        return Future.future(promise -> {
            List<Future<Void>> collect = computePrepareRollbackTargets().stream().map(c -> mySQLManager().getConnection(c).flatMap(function)).collect(Collectors.toList());
            CompositeFuture.join((List) collect)
                    .onComplete(event -> {
                        log.logRollback(xid, event.succeeded());
                        if (event.failed()) {
                            mySQLManager().setTimer(log.retryDelay(),
                                    () -> retryRollback(function).onComplete(promise));
                            return;
                        }
                        inTranscation = false;
                        clearConnections().onComplete(promise);
                    });
        });

    }

    /**
     * before XA_PREPARE,should log the participants
     */
    private void logParticipants() {
        ImmutableParticipantLog[] participantLogs = new ImmutableParticipantLog[map.size()];
        int index = 0;
        for (Map.Entry<String, NewMycatConnection> e : map.entrySet()) {
            participantLogs[index] = new ImmutableParticipantLog(e.getKey(),
                    log.getExpires(),
                    connectionState.get(e.getValue()));
            index++;
        }
        log.log(xid, participantLogs);
    }

    protected NewMycatConnection changeTo(NewMycatConnection c, State state) {
        connectionState.put(c, state);
        log.log(xid, getDatasourceName(c), state);
        return c;
    }

    protected void changeTo(String c, State state) {
        connectionState.put(map.get(c), state);
        log.log(xid, c, state);
    }


    @Override
    public Future<Void> commit() {
        return commitXa((unused) -> Future.succeededFuture());
    }

    /**
     * @param beforeCommit for the native connection commit or some exception test
     */
    public Future<Void> commitXa(Function<ImmutableCoordinatorLog, Future<Void>> beforeCommit) {
        return Future.future((Promise<Void> promsie) -> {
            logParticipants();
            Future<Void> xaEnd = executeTranscationConnection(connection -> {
                Future<Void> future = Future.succeededFuture();
                switch (connectionState.get(connection)) {
                    case XA_INITED:
                        future = future
                                .flatMap(unuse -> connection.update(String.format(XA_START, xid)))
                                .map(u -> changeTo(connection, State.XA_STARTED)).mapEmpty();
                    case XA_STARTED:
                        future = future
                                .flatMap(unuse -> connection.update(String.format(XA_END, xid)))
                                .map(u -> changeTo(connection, State.XA_ENDED)).mapEmpty();
                    case XA_ENDED:
                    default:
                }
                return future.mapEmpty();
            });
            xaEnd.onFailure(throwable -> promsie.tryFail(throwable));
            xaEnd.onSuccess(event -> {
                executeTranscationConnection(connection -> {
                    if (connectionState.get(connection) != State.XA_PREPARED) {
                        return connection.update(String.format(XA_PREPARE, xid))
                                .map(c -> changeTo(connection, State.XA_PREPARED)).mapEmpty();
                    }
                    return Future.succeededFuture();
                })
                        .onFailure(throwable -> {
                            log.logPrepare(xid, false);
                            //客户端触发回滚
                            promsie.tryFail(throwable);
                        })
                        .onSuccess(compositeFuture -> {
                            log.logPrepare(xid, true);
                            Future<Void> future;
                            try {
                                /**
                                 * if log commit fail ,occur exception,other transcations rollback.
                                 */
                                ImmutableCoordinatorLog coordinatorLog = this.log.logCommitBeforeXaCommit(xid);
                                /**
                                 * if native connection has inner commited,
                                 * but it didn't received the commit response.
                                 * should check the by manually.
                                 */
                                future = beforeCommit.apply(coordinatorLog);
                            } catch (Throwable throwable) {
                                future = Future.failedFuture(throwable);
                            }
                            future.onFailure((Handler<Throwable>) throwable -> {
                                log.logCancelCommitBeforeXaCommit(xid);
                                //客户端触发回滚
                                /**
                                 * the client received exception ,it must  rollback.
                                 */
                                promsie.fail(throwable);
                            });
                            future.onSuccess(event16 -> {
                                executeTranscationConnection(connection -> {
                                    return connection.update(String.format(XA_COMMIT, xid))
                                            .map(c -> changeTo(connection, State.XA_COMMITED)).mapEmpty();
                                })
                                        .onFailure(ignored -> {
                                            log.logCommit(xid, false);
                                            //retry
                                            retryCommit().onComplete(promsie);
                                        })
                                        .onSuccess(ignored -> {
                                            inTranscation = false;

                                            log.logCommit(xid, true);

                                            clearConnections().onComplete(promsie);
                                        });
                            });
                        });
            });
        });

    }


    /**
     * use new connection to retry the connection.
     */
    private Future<Void> retryCommit() {
        return Future.future((Promise<Void> promise) -> {
            CompositeFuture all = CompositeFuture.join(computePrepareCommittedTargets().stream()
                    .map(s -> mySQLManager().getConnection(s)
                            .compose(c -> {
                                return c.update(String.format(XA_COMMIT, xid))
                                        .compose(rows -> {
                                            changeTo(s, State.XA_COMMITED);
                                            return c.close();
                                        }, throwable -> {
                                            return c.close();
                                        });
                            })).collect(Collectors.toList()));
            all.onSuccess(event -> {
                inTranscation = false;
                log.logCommit(xid, true);
                clearConnections().onComplete(promise);
            });
            all.onFailure(event -> {
                mySQLManager().setTimer(log.retryDelay(), () -> retryCommit().onComplete(promise));
            });
        });
    }

    private List<String> computePrepareCommittedTargets() {
        List<String> collect = connectionState.entrySet().stream()
                .filter(i -> i.getValue() != State.XA_COMMITED)
                .map(i -> i.getKey())
                .map(k -> getDatasourceName(k)).collect(Collectors.toList());
        return collect;
    }

    private List<String> computePrepareRollbackTargets() {
        List<String> collect = connectionState.entrySet().stream()
                .filter(i -> i.getValue() != State.XA_ROLLBACKED)
                .map(i -> i.getKey())
                .map(k -> getDatasourceName(k)).collect(Collectors.toList());
        return collect;
    }

    public Future<Void> executeTranscationConnection(Function<NewMycatConnection, Future<Void>> connectionFutureFunction) {
        if (map.isEmpty()) {
            return Future.succeededFuture();
        }
        List<Future> futures = map.values().stream().map(connectionFutureFunction).collect(Collectors.toList());
        return CompositeFuture.join(futures).mapEmpty();
    }

    /**
     *
     */
    public Future<Void> close() {
        Function<NewMycatConnection, Future<Void>> consumer = newMycatConnection -> newMycatConnection.close();
        return close(consumer);
    }

    private Future<Void> close(Function<NewMycatConnection, Future<Void>> consumer) {
        Future<Void> allFuture = CompositeFuture.join((List) closeList).mapEmpty();
        closeList.clear();
        if (inTranscation) {
            allFuture = CompositeFuture.join(allFuture, rollback()).mapEmpty();
        }
        return allFuture.flatMap(unused -> {
            Future<Void> future = Future.succeededFuture();
            for (NewMycatConnection extraConnection : extraConnections) {
                future = future.compose(unused2 -> consumer.apply(extraConnection));
            }
            future = future.onComplete(event -> extraConnections.clear());
            return future.onComplete(u -> executeTranscationConnection(c -> {
                return consumer.apply(c);
            }).onComplete(c -> {
                map.clear();
                connectionState.clear();
            }));
        });
    }

    @Override
    public Future<Void> kill() {
        Function<NewMycatConnection, Future<Void>> consumer = newMycatConnection -> {
            newMycatConnection.abandonConnection();
            return Future.succeededFuture();
        };
        return close(consumer);
    }


    @Override
    public Future<Void> closeStatementState() {
        List<Future> stopResultSet = getAllConnections().stream().map(i -> i.abandonQuery()).collect(Collectors.toList());
        Future<Void> future = CompositeFuture.join(stopResultSet).mapEmpty();
        future = future.flatMap(unused -> CompositeFuture.join((List) closeList).mapEmpty());
        closeList.clear();
        return future.onComplete(event -> clearConnections().onComplete(unused -> {
            if (!inTranscation) {
                xid = null;
            }
        }));
    }

    @Override
    public String getXid() {
        return xid;
    }

    @Override
    public void addCloseFuture(Future<Void> future) {
        closeList.add(future);
    }

    @Override
    public List<NewMycatConnection> getAllConnections() {
        ArrayList<NewMycatConnection> resList = new ArrayList<>();
        resList.addAll(map.values());
        resList.addAll(extraConnections);
        return resList;
    }

    /**
     * before clear connections,it should check not be in transaction
     */
    public Future<Void> clearConnections() {
        Future<Void> future = CompositeFuture.join((List) closeList).mapEmpty();
        closeList.clear();
        for (NewMycatConnection extraConnection : extraConnections) {
            future = future.compose(unused -> extraConnection.close());
        }
        future = future.onComplete(event -> extraConnections.clear());
        if (inTranscation) {
            return future;
        } else {
            return future.onComplete(u -> executeTranscationConnection(c -> {
                return c.close();
            }).onComplete(c -> {
                map.clear();
                connectionState.clear();
            }));
        }
    }
}

