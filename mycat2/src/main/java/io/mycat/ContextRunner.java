/**
 * Copyright (C) <2020>  <chen junwen>
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
package io.mycat;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.UpdateRowIteratorResponse;
import io.mycat.beans.mycat.TransactionType;
import io.mycat.beans.mysql.MySQLFieldsType;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.beans.resultset.MycatResponse;
import io.mycat.beans.resultset.MycatResultSet;
import io.mycat.boost.UserBooster;
import io.mycat.calcite.prepare.MycatSQLPrepareObject;
import io.mycat.calcite.prepare.MycatSqlPlanner;
import io.mycat.calcite.prepare.MycatTextUpdatePrepareObject;
import io.mycat.client.Context;
import io.mycat.client.MycatClient;
import io.mycat.datasource.jdbc.JdbcRuntime;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.lib.impl.JdbcLib;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.metadata.LogicTableType;
import io.mycat.metadata.TableHandler;
import io.mycat.proxy.ResultSetProvider;
import io.mycat.proxy.session.MycatSession;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.resultset.TextResultSetResponse;
import io.mycat.runtime.TransactionSessionUtil;
import io.mycat.upondb.*;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.ToString;
import org.jetbrains.annotations.NotNull;

import java.nio.charset.Charset;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.mycat.SQLExecuterWriter.writeToMycatSession;

/**
 * @author chen junwen
 */
public class ContextRunner {

    public static final MycatLogger LOGGER = MycatLoggerFactory.getLogger(ContextRunner.class);
    //item
    public static final String SCHEMA_NAME = "schema";
    public static final String TARGETS = "targets";
    public static final String TRANSACTION_TYPE = "transactionType";
    public static final String EXECUTE_TYPE = "executeType";
    public static final String TRANSACTION_ISOLATION = "transactionIsolation";
    public static final String BALANCE = "balance";

    //inst command
    public static final String OK = "ok";
    public static final String ERROR = "error";
    public static final String EXPLAIN = "explainSql";
    public static final String EXPLAIN_HBT = "explainPlan";
    public static final String DISTRIBUTED_QUERY = ("distributedQuery");
    public static final String DISTRIBUTED_INSERT = ("distributedInsert");
    public static final String DISTRIBUTED_UPDATE = ("distributedUpdate");
    public static final String EXECUTE_PLAN = ("executePlan");
    public static final String USE_STATEMENT = ("useStatement");
    public static final String COMMIT = ("commit");
    public static final String BEGIN = ("begin");
    public static final String SET_TRANSACTION_ISOLATION = ("setTransactionIsolation");
    public static final String ROLLBACK = ("rollback");
    public static final String EXECUTE = ("execute");
    public static final String MYCAT_DB = ("mycatdb");
    //    public static final String PASS = ("pass");
    //    public static final String SET_TRANSACTION_TYPE = ("setTransactionType");
    public static final String ON_XA = ("onXA");
    public static final String OFF_XA = ("offXA");
    public static final String SET_AUTOCOMMIT_OFF = ("setAutoCommitOff");
    public static final String SET_AUTOCOMMIT_ON = ("setAutoCommitOn");
    public static final String SELECT_LAST_INSERT_ID = ("selectLastInsertId");
    public static final String SELECT_AUTOCOMMIT = ("selectAutocommit");
    public static final String SELECT_READ_ONLY = ("selectTransactionReadOnly");
    static final ConcurrentHashMap<String, Command> COMMANDS;

    public static void run(MycatClient client, Context analysis, MycatSession session) {
        Command command = Objects.requireNonNull(COMMANDS.getOrDefault(analysis.getCommand().toLowerCase(), ERROR_COMMAND));
        Runnable apply = command.apply(client, analysis, session);
        apply.run();
    }


    @ToString
    static class Details {
        ExecuteType executeType;
        Map<String, List<String>> targets;
        String balance;
        final boolean forceProxy;
        final boolean needStartTransaction;
        boolean globalTableUpdate;


        public Details(ExecuteType executeType, Map<String, List<String>> backendTableInfos, boolean isGlobal, boolean forceProxy, boolean needStartTransaction) {
            this.executeType = executeType;
            this.targets = backendTableInfos;
            this.globalTableUpdate = isGlobal;
            this.forceProxy = forceProxy;
            this.needStartTransaction = needStartTransaction;
            this.balance = null;
            this.globalTableUpdate = false;
        }

        public <K, V> Details(ExecuteType executeType, Map<String, List<String>> backendTableInfos, String balance, boolean forceProxy, boolean needStartTransaction) {
            this.executeType = executeType;
            this.targets = backendTableInfos;
            this.balance = balance;
            this.forceProxy = forceProxy;
            this.needStartTransaction = needStartTransaction;
            this.globalTableUpdate = false;
        }

        public List<String> toExplain() {
            ArrayList<String> list = new ArrayList<>();
            list.add("executeType = " + executeType);
            for (Map.Entry<String, List<String>> stringListEntry : targets.entrySet()) {
                for (String s : stringListEntry.getValue()) {
                    list.add("target: " + stringListEntry.getKey() + " sql:" + s);
                }
            }
            list.add("balance = " + balance);
            list.add("globalTableUpdate = " + globalTableUpdate);
            list.add("needStartTransaction = " + needStartTransaction);
            list.add("forceProxy = " + forceProxy);
            return list;
        }
    }

    public static final Command ERROR_COMMAND = new Command() {
        @Override
        public Runnable apply(MycatClient client, Context context, MycatSession session) {
            String errorMessage = context.getVariable("errorMessage", "may be unknown command");
            int errorCode = Integer.parseInt(context.getVariable("errorCode", "-1"));
            return new Runnable() {
                @Override
                public void run() {
                    session.setLastMessage(errorMessage);
                    session.setLastErrorCode(errorCode);
                    session.writeErrorEndPacketBySyncInProcessError();
                }
            };
        }

        @Override
        public Runnable explain(MycatClient client, Context context, MycatSession session) {
            String errorMessage = context.getVariable("errorMessage", "may be unknown command");
            int errorCode = Integer.parseInt(context.getVariable("errorCode", "-1"));
            return () -> {
                writePlan(session, MessageFormat.format("errorMessage:{0} errorCode:{1}", errorMessage, errorCode));
            };
        }
    };
    public static final Command OK_COMMAND = new Command() {
        @Override
        public Runnable apply(MycatClient client, Context context, MycatSession session) {
            return new Runnable() {
                @Override
                public void run() {
                    session.writeOkEndPacket();
                }
            };
        }

        @Override
        public Runnable explain(MycatClient client, Context context, MycatSession session) {
            return () -> {
                writePlan(session, MessageFormat.format("sql:{0}", context.getExplain()));
            };
        }
    };

    static {
        COMMANDS = new ConcurrentHashMap<>();
        COMMANDS.put(ERROR, ERROR_COMMAND);
        COMMANDS.put(OK, OK_COMMAND);
        /**
         * 参数:statement
         */
        COMMANDS.put(EXPLAIN, new Command() {
            @Override
            public Runnable apply(MycatClient client, Context context, MycatSession session) {
                String sql = context.getVariable("statement");
                Context analysis = client.analysis(sql);
                Command command = COMMANDS.get(analysis.getCommand());
                return command.explain(client, analysis, session);
            }

            @Override
            public Runnable explain(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    writePlan(session, "explainSql statement");
                };
            }
        });

        /**
         * 参数:无
         */
        COMMANDS.put(SELECT_LAST_INSERT_ID, new Command() {
            String columnName = "last_insert_id()";

            @Override
            public Runnable apply(MycatClient client, Context context, MycatSession session) {
                MycatResultSet defaultResultSet = ResultSetProvider.INSTANCE.createDefaultResultSet(1, 33, Charset.defaultCharset());
                String lastInsertId = String.valueOf(session.getLastInsertId());
                defaultResultSet.addColumnDef(0, columnName, MySQLFieldsType.FIELD_TYPE_LONGLONG);
                defaultResultSet.addTextRowPayload(lastInsertId);
                return () -> SQLExecuterWriter.writeToMycatSession(session, defaultResultSet);
            }

            @Override
            public Runnable explain(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    writePlan(session, columnName + ":" + session.getLastInsertId());
                };
            }
        });
        /**
         * 参数:无
         */
        COMMANDS.put(SELECT_AUTOCOMMIT, new Command() {
            String columnName = "@@session.autocommit";

            @Override
            public Runnable apply(MycatClient client, Context context, MycatSession session) {
                MycatResultSet defaultResultSet = ResultSetProvider.INSTANCE.createDefaultResultSet(1, 33, Charset.defaultCharset());
                String isAutocommit = session.isAutocommit() ? "1" : "0";
                defaultResultSet.addColumnDef(0, columnName, MySQLFieldsType.FIELD_TYPE_LONGLONG);
                defaultResultSet.addTextRowPayload(isAutocommit);
                return () -> SQLExecuterWriter.writeToMycatSession(session, defaultResultSet);
            }

            @Override
            public Runnable explain(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    writePlan(session, columnName + ":" + session.isAutocommit());
                };
            }
        });
        /**
         * 参数:无
         */
        COMMANDS.put(SELECT_READ_ONLY, new Command() {

            @Override
            public Runnable apply(MycatClient client, Context context, MycatSession session) {
                String columnName = context.getVariable("columnName");
                MycatResultSet defaultResultSet = ResultSetProvider.INSTANCE.createDefaultResultSet(1, 33, Charset.defaultCharset());
                String isReadOnly = session.getDataContext().isReadOnly() ? "1" : "0";
                defaultResultSet.addColumnDef(0, columnName, MySQLFieldsType.FIELD_TYPE_LONGLONG);
                defaultResultSet.addTextRowPayload(isReadOnly);
                return () -> SQLExecuterWriter.writeToMycatSession(session, defaultResultSet);
            }

            @Override
            public Runnable explain(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    writePlan(session, SELECT_READ_ONLY + ":" + session.getDataContext().isReadOnly());
                };
            }
        });

        /**
         * 参数:接收的sql
         */
        COMMANDS.put(DISTRIBUTED_QUERY, new Command() {
            @Override
            public Runnable apply(MycatClient client, Context context, MycatSession session) {
                return () -> block(session, mycat -> {
                    String defaultSchema = client.getDefaultSchema();
                    String explain = context.getExplain().trim();
                    if (explain.endsWith(";")) {
                        explain = explain.substring(0, explain.length() - 1);
                    }
                    LOGGER.debug("session id:{} action: plan {}", session.sessionId(), explain);
                    MycatDBClientMediator client1 = MycatDBs.createClient(session.unwrap(MycatDataContext.class));
                    client1.useSchema(defaultSchema);
                    MycatSQLPrepareObject mycatSQLPrepareObject = client1.getUponDBSharedServer().innerQueryPrepareObject(client1.sqlContext().simplySql(explain), client1);
                    PlanRunner plan = mycatSQLPrepareObject.plan(Collections.emptyList());
                    switch (client.getTransactionType()) {
                        case PROXY_TRANSACTION_TYPE: {
                            if (plan instanceof MycatSqlPlanner) {
                                MycatSqlPlanner plan1 = (MycatSqlPlanner) plan;
                                ProxyInfo proxyInfo = plan1.tryGetProxyInfo();
                                if (proxyInfo != null) {
                                    MySQLTaskUtil.proxyBackendByTargetName(session, proxyInfo.getTargetName(), proxyInfo.getSql(),
                                            MySQLTaskUtil.TransactionSyncType.create(session.isAutocommit(), session.isInTransaction()),
                                            session.getIsolation(), proxyInfo.isUpdateOpt(), null);
                                    return;
                                }
                            }
                            break;
                        }
                    }
                    RowBaseIterator query = client1.query(explain);
                    TextResultSetResponse connection = new TextResultSetResponse(query);
                    SQLExecuterWriter.writeToMycatSession(mycat, new MycatResponse[]{connection});
                    client1.recycleResource();//移除已经关闭的连接,
                });
            }


            @Override
            public Runnable explain(MycatClient client, Context context, MycatSession session) {

                String defaultSchema = client.getDefaultSchema();
                String command = context.getExplain();

                return () -> block(session, mycat -> {
                    MycatDBClientMediator client1 = MycatDBs.createClient(session.unwrap(MycatDataContext.class));
                    client1.useSchema(defaultSchema);
                    PrepareObject prepare = client1.prepare(command);
                    List<String> explain = prepare.plan(Collections.emptyList()).explain();
                    writePlan(session, explain);
                });
            }
        });

        /**
         * 参数:接收的sql
         */
        COMMANDS.put(MYCAT_DB, new Command() {
            @Override
            public Runnable apply(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    block(session, mycat -> {
                        String explain = context.getExplain();
                        MycatDBClientApi mycatDb = client.getMycatDb();
                        Iterator<RowBaseIterator> rowBaseIteratorIterator = mycatDb.executeSqls(explain);
                        ArrayList<MycatResponse> responses = new ArrayList<>();
                        while (rowBaseIteratorIterator.hasNext()) {
                            RowBaseIterator next = rowBaseIteratorIterator.next();
                            if (next instanceof UpdateRowIteratorResponse) {
                                UpdateRowIteratorResponse next1 = (UpdateRowIteratorResponse) next;
                                next.next();
                                responses.add(new UpdateRowIteratorResponse((int) next1.getUpdateCount(), next1.getLastInsertId(), mycatDb.getServerStatus()));
                            } else {
                                TextResultSetResponse next1 = new TextResultSetResponse(next);
                                responses.add(next1);
                            }
                        }
                        SQLExecuterWriter.writeToMycatSession(mycat, responses);
                        mycatDb.recycleResource();//移除已经关闭的连接,
                    });
                };
            }

            @Override
            public Runnable explain(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    block(session, mycat -> {
                        MycatDBClientApi mycatDb = client.getMycatDb();
                        writePlan(session, mycatDb.explain(context.getExplain()));
                        mycatDb.recycleResource();//移除已经关闭的连接,
                    });
                    return;
                };
            }
        });

        /**
         * 参数:接收的sql
         */
        COMMANDS.put(EXECUTE_PLAN, new Command() {
            @Override
            public Runnable apply(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    block(session, mycat -> {
                        String explain = context.getExplain();
                        MycatDBClientApi mycatDb = client.getMycatDb();
                        RowBaseIterator rowBaseIterator = mycatDb.executeRel(explain);
                        TextResultSetResponse connection = new TextResultSetResponse(rowBaseIterator);
                        SQLExecuterWriter.writeToMycatSession(mycat, new MycatResponse[]{connection});
                        mycatDb.recycleResource();//移除已经关闭的连接,
                    });
                };
            }

            @Override
            public Runnable explain(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    block(session, mycat -> {
                        MycatDBClientApi mycatDb = client.getMycatDb();
                        RowBaseIterator rowBaseIterator = mycatDb.executeRel(context.getExplain());
                        SQLExecuterWriter.writeToMycatSession(mycat, new MycatResponse[]{new TextResultSetResponse(rowBaseIterator)});
                        mycatDb.recycleResource();//移除已经关闭的连接,
                    });
                    return;
                };
            }
        });

        /**
         * 参数:接收的sql
         */
        COMMANDS.put(EXPLAIN_HBT, new Command() {
            @Override
            public Runnable apply(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    block(session, mycat -> {
                        MycatDBClientApi mycatDb = client.getMycatDb();
                        String explain = context.getExplain();
                        RowBaseIterator rowBaseIterator = mycatDb.executeRel(explain);
                        SQLExecuterWriter.writeToMycatSession(mycat, new MycatResponse[]{new TextResultSetResponse(rowBaseIterator)});
                        mycatDb.recycleResource();//移除已经关闭的连接,
                    });
                };
            }

            @Override
            public Runnable explain(MycatClient client, Context context, MycatSession session) {
                throw new UnsupportedOperationException();
            }

        });

        /**
         * 参数:SCHEMA_NAME
         */
        COMMANDS.put(USE_STATEMENT, new Command() {
                    @Override
                    public Runnable apply(MycatClient client, Context context, MycatSession session) {
                        return () -> {
                            String schemaName = Objects.requireNonNull(context.getVariable(SCHEMA_NAME));
                            client.useSchema(schemaName);
                            session.setSchema(schemaName);
                            LOGGER.debug("session id:{} action: use {}", session.sessionId(), schemaName);
                            session.writeOkEndPacket();
                        };
                    }

                    @Override
                    public Runnable explain(MycatClient client, Context context, MycatSession session) {
                        return () -> {
                            String schemaName = Objects.requireNonNull(context.getVariable(SCHEMA_NAME));
                            writePlan(session, "use " + schemaName);
                        };
                    }
                }
        );
        /**
         * 参数:无
         */
        COMMANDS.put(ON_XA, new Command() {
            @Override
            public Runnable apply(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    if (session.isInTransaction()) throw new IllegalArgumentException();
                    client.useTransactionType(TransactionType.JDBC_TRANSACTION_TYPE);
                    LOGGER.debug("session id:{} action:{}", session.sessionId(), "set xa = 1 exe success");
                    session.writeOkEndPacket();
                };
            }

            @Override
            public Runnable explain(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    writePlan(session, context.getVariable(TRANSACTION_TYPE));
                };
            }
        });
        /**
         * 参数:无
         */
        COMMANDS.put(OFF_XA, new Command() {
            @Override
            public Runnable apply(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    if (session.isInTransaction()) throw new IllegalArgumentException();
                    client.useTransactionType(TransactionType.PROXY_TRANSACTION_TYPE);
                    LOGGER.debug("session id:{} action:{}", session.sessionId(), "set xa = 0 exe success");
                    session.writeOkEndPacket();
                };
            }

            @Override
            public Runnable explain(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    writePlan(session, context.getVariable(TRANSACTION_TYPE));
                };
            }
        });
        /**
         * 参数:无
         */
        COMMANDS.put(SET_AUTOCOMMIT_OFF, new Command() {
            @Override
            public Runnable apply(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    session.setAutoCommit(false);
                    LOGGER.debug("session id:{} action:set autocommit = 0 exe success", session.sessionId());
                    session.writeOkEndPacket();
                };
            }

            @Override
            public Runnable explain(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    writePlan(session, "SET_AUTOCOMMIT_OFF");
                };
            }
        });
        /**
         * 参数:无
         */
        COMMANDS.put(SET_AUTOCOMMIT_ON, new Command() {
            @Override
            public Runnable apply(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    session.setAutoCommit(true);
                    LOGGER.debug("session id:{} action:set autocommit = 1 exe success", session.sessionId());
                    session.writeOkEndPacket();
                };
            }

            @Override
            public Runnable explain(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    writePlan(session, "SET_AUTOCOMMIT_ON");
                };
            }
        });
        /**
         * 参数:无
         */
        COMMANDS.put(BEGIN, new Command() {
            @Override
            public Runnable apply(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    session.setInTranscation(true);
                    LOGGER.debug("session id:{} action:{}", session.sessionId(), "begin exe success");
                    session.writeOkEndPacket();
                };
            }

            @Override
            public Runnable explain(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    writePlan(session, "BEGIN");
                };
            }
        });
        /**
         * 参数:transactionIsolation
         */
        COMMANDS.put(SET_TRANSACTION_ISOLATION, new Command() {
            @Override
            public Runnable apply(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    TransactionType transactionType = client.getTransactionType();
                    MySQLIsolation mySQLIsolation = MySQLIsolation.parse(Objects.requireNonNull(context.getVariable(TRANSACTION_ISOLATION)));
                    session.setIsolation(mySQLIsolation);
                    LOGGER.debug("session id:{} action: set isolation = {}", session.sessionId(), mySQLIsolation);
                    session.writeOkEndPacket();
                };
            }

            @Override
            public Runnable explain(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    writePlan(session, Objects.requireNonNull(context.getVariable(TRANSACTION_ISOLATION)));
                };
            }
        });
        /**
         * 参数:无
         */
        COMMANDS.put(ROLLBACK, new Command() {
            @Override
            public Runnable apply(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    TransactionType transactionType = client.getTransactionType();

                    switch (transactionType) {
                        case PROXY_TRANSACTION_TYPE:
                            if (session.isBindMySQLSession()) {
                                session.setInTranscation(false);
                                MySQLTaskUtil.proxyBackend(session, "ROLLBACK");
                                LOGGER.debug("session id:{} action: rollback from binding session", session.sessionId());
                                return;
                            } else {
                                session.setInTranscation(false);
                                session.writeOkEndPacket();
                                LOGGER.debug("session id:{} action: rollback from unbinding session", session.sessionId());
                                return;
                            }
                        case JDBC_TRANSACTION_TYPE:
                            block(session, mycat -> {
                                TransactionSession transactionSession = session.getDataContext().getTransactionSession();
                                transactionSession.rollback();
                                LOGGER.debug("session id:{} action: rollback from xa", session.sessionId());
                                mycat.writeOkEndPacket();
                            });
                            return;
                    }
                };
            }

            @Override
            public Runnable explain(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    writePlan(session, "ROLLBACK");
                };
            }
        });

        /**
         * 参数:无
         */
        COMMANDS.put(COMMIT, new Command() {
            @Override
            public Runnable apply(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    TransactionType transactionType = client.getTransactionType();
                    switch (transactionType) {
                        case PROXY_TRANSACTION_TYPE:
                            if (!session.isBindMySQLSession()) {
                                session.setInTranscation(false);
                                LOGGER.debug("session id:{} action: commit from unbinding session", session.sessionId());
                                session.writeOkEndPacket();
                                return;
                            } else {
                                session.setInTranscation(false);
                                MySQLTaskUtil.proxyBackend(session, "COMMIT");
                                LOGGER.debug("session id:{} action: commit from binding session", session.sessionId());
                                return;
                            }
                        case JDBC_TRANSACTION_TYPE:
                            block(session, mycat -> {
                                TransactionSession transactionSession = session.getDataContext().getTransactionSession();
                                transactionSession.commit();
                                LOGGER.debug("session id:{} action: commit from xa", session.sessionId());
                                session.setInTranscation(false);
                                mycat.writeOkEndPacket();
                            });
                    }
                };
            }

            @Override
            public Runnable explain(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    writePlan(session, "COMMIT");
                };
            }
        });
        /**
         * 参数:
         * balance
         * targets
         * executeType:
         * metaData:true:false
         * forceProxy:true:false
         * needTransaction:true|false
         */
        Command execute = new Command() {
            @Override
            public Runnable apply(MycatClient client, Context context, MycatSession session) {
                Details details = explainDetail(client, context, session);
                return execute(client, session, details);
            }

            @NotNull
            private ContextRunner.Details explainDetail(MycatClient client, Context context, MycatSession session) {
                boolean forceProxy = "true".equalsIgnoreCase(context.getVariable("forceProxy", "false"));
                boolean needTransactionConfig = "true".equalsIgnoreCase(context.getVariable("needTransaction", "true"));
                boolean metaData = "true".equalsIgnoreCase(context.getVariable("metaData", "false"));
                boolean needStartTransaction = needTransactionConfig && (!session.isAutocommit() || session.isInTransaction());
                String explain = context.getExplain();//触发注解解析并缓存
                String balance = context.getVariable(BALANCE);
                ExecuteType executeType = ExecuteType.valueOf(context.getVariable(EXECUTE_TYPE, ExecuteType.DEFAULT.name()));
                Details details;
                if (metaData) {
                    Context.SchemaTableObject tableForUpdateOpt = context.getTableForUpdateOpt();
                    details = getDetails(client, needStartTransaction, balance, tableForUpdateOpt, executeType, explain, forceProxy, needStartTransaction);
                } else {
                    String replicaName = ReplicaSelectorRuntime.INSTANCE.getDatasourceNameByReplicaName(
                            Objects.requireNonNull(context.getVariable(TARGETS), "can not get " + TARGETS + " of " + context.getName()),
                            needStartTransaction, balance);
                    details = new Details(executeType, Collections.singletonMap(replicaName, Collections.singletonList(explain)), balance, forceProxy, needStartTransaction);
                }
                return details;
            }

            @Override
            public Runnable explain(MycatClient client, Context context, MycatSession session) {
                Details details = explainDetail(client, context, session);
                return () -> writePlan(session, details.toExplain());
            }
        };
        COMMANDS.put(EXECUTE, execute);

        Function<ExecuteType, Command> executeSupplier = (ExecuteType executeType) -> new Command() {
            @Override
            public Runnable apply(MycatClient client, Context context, MycatSession session) {
                boolean startTransaction = !session.isAutocommit() || session.isInTransaction();
                Details details = getDetails(client, true, null, context.getTableForUpdateOpt(), executeType, context.getExplain(), false, startTransaction);
                return execute(client, session, details);
            }

            @Override
            public Runnable explain(MycatClient client, Context context, MycatSession session) {
                boolean startTransaction = !session.isAutocommit() || session.isInTransaction();
                Details details = getDetails(client, true, null, context.getTableForUpdateOpt(), executeType, context.getExplain(), false, startTransaction);
                return () -> writePlan(session, details.toExplain());
            }
        };
        COMMANDS.put(DISTRIBUTED_UPDATE, executeSupplier.apply(ExecuteType.INSERT));
        COMMANDS.put(DISTRIBUTED_INSERT, executeSupplier.apply(ExecuteType.UPDATE));
        Set<String> supportCommands = UserBooster.getSupportCommands();
        for (Map.Entry<String, Command> stringCommandEntry : COMMANDS.entrySet()) {
            Command value = stringCommandEntry.getValue();
            if (supportCommands.contains(stringCommandEntry.getKey())) {
                value = new CacheCommandWrapper(stringCommandEntry.getValue());
            }
            COMMANDS.put(stringCommandEntry.getKey().toLowerCase(), value);
        }
    }

    @NotNull
    private static Runnable execute(MycatClient client, MycatSession session, final Details details) {
        Map<String, List<String>> tasks = details.targets;
        String balance = details.balance;
        ExecuteType executeType = details.executeType;
        MySQLIsolation isolation = session.getIsolation();

        LOGGER.debug("session id:{} execute :{}", session.sessionId(), details.toString());


        if (details.globalTableUpdate & (client.getTransactionType() == TransactionType.PROXY_TRANSACTION_TYPE || details.forceProxy)) {
            return () -> block(session, (mycat -> {
                Map<String, List<String>> targets = details.targets;
                if (targets.isEmpty()) {
                    throw new AssertionError();
                }
                int count = targets.size();
                String targetName = null;
                String sql = null;
                for (Map.Entry<String, List<String>> stringListEntry : targets.entrySet()) {
                    if (count == 1) {
                        targetName = stringListEntry.getKey();
                        List<String> value = stringListEntry.getValue();
                        if (value.size() != 1) {
                            List<String> strings = value.subList(1, value.size());
                            try (DefaultConnection connection = JdbcRuntime.INSTANCE.getConnection(stringListEntry.getKey())) {
                                for (String s : strings) {
                                    connection.executeUpdate(s, true, 0);
                                }
                            }
                        }
                        sql = value.get(0);
                        break;
                    } else {
                        try (DefaultConnection connection = JdbcRuntime.INSTANCE.getConnection(stringListEntry.getKey())) {
                            for (String s : stringListEntry.getValue()) {
                                connection.executeUpdate(s, true, 0);
                            }
                        }
                        count--;
                    }
                }
                MySQLTaskUtil.proxyBackendByTargetName(session, targetName, sql,
                        MySQLTaskUtil.TransactionSyncType.create(session.isAutocommit(), session.isInTransaction()),
                        session.getIsolation(), details.executeType.isMaster(), balance);
            }));
        }
        boolean runOnProxy = isOne(tasks) && client.getTransactionType() == TransactionType.PROXY_TRANSACTION_TYPE || details.forceProxy;
        if (runOnProxy) {
            if (tasks.size() != 1) throw new IllegalArgumentException();
            String[] strings = checkThenGetOne(tasks);
            return () -> {
                MycatDataContext dataContext = session.getDataContext();
                MySQLTaskUtil.proxyBackendByTargetName(session, strings[0], strings[1],
                        MySQLTaskUtil.TransactionSyncType.create(session.isAutocommit(), session.isInTransaction()),
                        session.getIsolation(), details.executeType.isMaster(), balance);
            };
        } else {
            return () -> {
                block(session, mycat -> {
                            TransactionSession transactionSession = session.getDataContext().getTransactionSession();
                            if (details.needStartTransaction) {
                                LOGGER.debug("session id:{} startTransaction", session.sessionId());
                                // TransactionSessionUtil.reset();
                                transactionSession.setTransactionIsolation(isolation.getJdbcValue());
                                transactionSession.begin();
                                session.setInTranscation(true);
                            }
                            switch (executeType) {
                                case QUERY_MASTER:
                                case QUERY: {
                                    Map<String, List<String>> backendTableInfos = details.targets;
                                    String[] infos = checkThenGetOne(backendTableInfos);
                                    writeToMycatSession(session, TransactionSessionUtil.executeQuery(transactionSession, infos[0], infos[1]));
                                    return;
                                }

                                case INSERT:
                                case UPDATE:
                                    writeToMycatSession(session, TransactionSessionUtil.executeUpdateByDatasouce(transactionSession, tasks, true));
                                    return;
                            }
                            throw new IllegalArgumentException();
                        }
                );
            };
        }
    }


    private static String[] checkThenGetOne(Map<String, List<String>> backendTableInfos) {
        if (backendTableInfos.size() != 1) {
            throw new IllegalArgumentException();
        }
        Map.Entry<String, List<String>> next = backendTableInfos.entrySet().iterator().next();
        List<String> list = next.getValue();
        if (list.size() != 1) {
            throw new IllegalArgumentException();
        }
        return new String[]{next.getKey(), list.get(0)};
    }

    private static boolean isOne(Map<String, List<String>> backendTableInfos) {
        if (backendTableInfos.size() != 1) {
            return false;
        }
        Map.Entry<String, List<String>> next = backendTableInfos.entrySet().iterator().next();
        List<String> list = next.getValue();
        return list.size() == 1;
    }

    private static void writePlan(MycatSession session, String message) {
        writePlan(session, Arrays.asList(message));
    }

    private static void writePlan(MycatSession session, List<String> messages) {
        MycatResultSet defaultResultSet = ResultSetProvider.INSTANCE.createDefaultResultSet(1, 33, Charset.defaultCharset());
        defaultResultSet.addColumnDef(0, "plan", MySQLFieldsType.FIELD_TYPE_VAR_STRING);
        messages.stream().map(i -> i.replaceAll("\n", " ")).forEach(defaultResultSet::addTextRowPayload);
        SQLExecuterWriter.writeToMycatSession(session, defaultResultSet);
    }

    public static void block(MycatSession mycat, Runner runner) {
        JdbcLib.block(mycat, runner);
    }


    @FunctionalInterface
    public static interface Runner extends Consumer<MycatSession> {
        void on(MycatSession mycat) throws Exception;

        @SneakyThrows
        @Override
        default void accept(MycatSession session) {
            on(session);
        }
    }

    @Getter
    enum ExecuteType {
        QUERY(false),
        QUERY_MASTER(true),
        INSERT(true),
        UPDATE(true),
//        RANDOM_QUERY(false),
//        BROADCAST_UPDATE(true),
        ;
        private boolean master;

        public static ExecuteType DEFAULT = ExecuteType.QUERY;

        ExecuteType(boolean master) {
            this.master = master;
        }

    }

    static Details getDetails(MycatClient client,
                              boolean needStartTransaction,
                              String balance,
                              Context.SchemaTableObject tableForUpdateOpt,
                              ExecuteType executeType,
                              String explain, boolean forceProxy, boolean startTransaction) {
        String schemaName = tableForUpdateOpt.getSchema();
        String tableName = tableForUpdateOpt.getTable();
        MycatDBClientMediator mycatDb = client.getMycatDb();
        TableHandler tableHandler = mycatDb.config().getTable(schemaName, tableName);
        boolean isGlobal = tableHandler.getType() == LogicTableType.GLOBAL;
        boolean master = executeType != ExecuteType.QUERY || needStartTransaction;
        MycatTextUpdatePrepareObject mycatTextUpdatePrepareObject = mycatDb.getUponDBSharedServer().innerUpdatePrepareObject(explain, mycatDb);
        Map<String, List<String>> routeMap = mycatTextUpdatePrepareObject.getRouteMap();
        return new Details(executeType, resolveDataSourceName(balance, master, routeMap), isGlobal, forceProxy, startTransaction);
    }

    @NotNull
    private static HashMap<String, List<String>> resolveDataSourceName(String balance, boolean master, Map<String, List<String>> routeMap) {
        HashMap<String, List<String>> map = new HashMap<>();
        for (Map.Entry<String, List<String>> entry : routeMap.entrySet()) {
            String datasourceNameByReplicaName = ReplicaSelectorRuntime.INSTANCE.getDatasourceNameByReplicaName(entry.getKey(), master, balance);
            List<String> list = map.computeIfAbsent(datasourceNameByReplicaName, s -> new ArrayList<>(1));
            list.addAll(entry.getValue());
        }
        return map;
    }
}