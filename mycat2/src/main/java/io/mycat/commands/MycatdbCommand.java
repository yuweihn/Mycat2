package io.mycat.commands;

import com.alibaba.fastsql.DbType;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLStartTransactionStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlExplainStatement;
import com.alibaba.fastsql.sql.parser.SQLParserUtils;
import com.alibaba.fastsql.sql.parser.SQLStatementParser;
import com.google.common.collect.ImmutableClassToInstanceMap;
import io.mycat.MycatDataContext;
import io.mycat.ReceiverImpl;
import io.mycat.hbt4.DefaultDatasourceFactory;
import io.mycat.hbt4.ExecutorImplementor;
import io.mycat.hbt4.ResponseExecutorImplementor;
import io.mycat.hbt4.executor.TempResultSetFactoryImpl;
import io.mycat.proxy.session.MycatSession;
import io.mycat.sqlhandler.SQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.sqlhandler.dcl.*;
import io.mycat.sqlhandler.ddl.*;
import io.mycat.sqlhandler.dml.*;
import io.mycat.sqlhandler.dql.*;
import io.mycat.util.Response;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Objects;

/**
 * @author Junwen Chen
 **/
public enum  MycatdbCommand {
    INSTANCE;
    final  Logger logger = LoggerFactory.getLogger(MycatdbCommand.class);
      ImmutableClassToInstanceMap<SQLHandler> sqlHandlerMap;


    MycatdbCommand() {
        try {
            final HashSet<SQLHandler> sqlHandlers = new HashSet<>();
            sqlHandlers.add(new SelectSQLHandler());
            sqlHandlers.add(new InsertSQLHandler());
            sqlHandlers.add(new DeleteSQLHandler());
            sqlHandlers.add(new UpdateSQLHandler());
            sqlHandlers.add(new TruncateSQLHandler());
            sqlHandlers.add(new ReplaceSQLHandler());
            sqlHandlers.add(new SetSQLHandler());
            sqlHandlers.add(new CommitSQLHandler());
            sqlHandlers.add(new KillSQLHandler());
            sqlHandlers.add(new RollbackSQLHandler());
            sqlHandlers.add(new SetTransactionSQLHandler());
            sqlHandlers.add(new StartTransactionSQLHandler());
            sqlHandlers.add(new HintSQLHandler());
            sqlHandlers.add(new UseSQLHandler());
            sqlHandlers.add(new LoadDataInFileSQLHandler());

            sqlHandlers.add(new AlterDatabaseSQLHandler());
            sqlHandlers.add(new AlterTableSQLHandler());
            sqlHandlers.add(new CreateDatabaseSQLHandler());
            sqlHandlers.add(new CreateIndexSQLHandler());
            sqlHandlers.add(new CreateTableSQLHandler());
            sqlHandlers.add(new CreateViewSQLHandler());
            sqlHandlers.add(new DropDatabaseSQLHandler());
            sqlHandlers.add(new DropTableSQLHandler());
            sqlHandlers.add(new DropViewSQLHandler());
            sqlHandlers.add(new RenameTableSQLHandler());

            //explain
            sqlHandlers.add(new ExplainSQLHandler());

            //show
            sqlHandlers.add(new ShowCharacterSetSQLHandler());
            sqlHandlers.add(new ShowCollationSQLHandler());
            sqlHandlers.add(new ShowColumnsSQLHandler());
            sqlHandlers.add(new ShowCreateTableSQLHandler());
            sqlHandlers.add(new ShowDatabasesHanlder());
            sqlHandlers.add(new ShowDatabaseSQLHandler());
            sqlHandlers.add(new ShowDatabaseStatusSQLHandler());
            sqlHandlers.add(new ShowEnginesSQLHandler());
            sqlHandlers.add(new ShowErrorsSQLHandler());
            sqlHandlers.add(new ShowIndexesSQLHandler());
            sqlHandlers.add(new ShowProcedureStatusSQLHandler());
            sqlHandlers.add(new ShowProcessListSQLHandler());
            sqlHandlers.add(new ShowStatusSQLHandler());
            sqlHandlers.add(new ShowTablesSQLHandler());
            sqlHandlers.add(new ShowTableStatusSQLHandler());
            sqlHandlers.add(new ShowVariantsSQLHandler());
            sqlHandlers.add(new ShowWarningsSQLHandler());
            sqlHandlers.add(new ShowCreateFunctionHanlder());
            sqlHandlers.add(new CreateTableSQLHandler());
            //Analyze
            sqlHandlers.add(new AnalyzeHanlder());

            ImmutableClassToInstanceMap.Builder<SQLHandler> builder = ImmutableClassToInstanceMap.builder();
            for (SQLHandler sqlHandler : sqlHandlers) {
                Class statementClass = sqlHandler.getStatementClass();
                Objects.requireNonNull(statementClass);
                builder.put(statementClass, sqlHandler);
            }
            sqlHandlerMap = builder.build();
        }catch (Throwable e){
            logger.error("",e);
        }
    }

    public    void executeQuery(String text, MycatSession session, MycatDataContext dataContext) {
        try {
            if (isHbt(text)) {
                executeHbt(dataContext, text.substring(12), new ReceiverImpl(session, 1, false, false));
                return;
            }
            logger.info(text);
            LinkedList<SQLStatement> statements = parse(text);
            Response receiver;
            if (statements.size() == 1 && statements.get(0) instanceof MySqlExplainStatement) {
                receiver = new ReceiverImpl(session, statements.size(), false, false);
            } else {
                receiver = new ReceiverImpl(session, statements.size(), false, false);
            }
            for (SQLStatement sqlStatement : statements) {
                SQLRequest<SQLStatement> request = new SQLRequest<>(sqlStatement);
                Class aClass = sqlStatement.getClass();
                SQLHandler instance = sqlHandlerMap.getInstance(aClass);
                if (instance == null) {
                    receiver.tryBroadcastShow(text);
                } else {
                    instance.execute(request, dataContext, receiver);
                }
            }
        } catch (Throwable e) {
            session.setLastMessage(e);
            session.writeErrorEndPacketBySyncInProcessError();
            return;
        }

    }

    private static boolean isHbt(String text) {
        boolean hbt = false;
        char c = text.charAt(0);
        if ((c == 'e' || c == 'E') && text.length() > 12) {
            hbt = "execute plan".equalsIgnoreCase(text.substring(0, 12));
        }
        return hbt;
    }

    @SneakyThrows
    private static void executeHbt(MycatDataContext dataContext, String substring, Response receiver) {
        DefaultDatasourceFactory datasourceFactory = new DefaultDatasourceFactory(dataContext);
        TempResultSetFactoryImpl tempResultSetFactory = new TempResultSetFactoryImpl();
        ExecutorImplementor executorImplementor = new ResponseExecutorImplementor(datasourceFactory, tempResultSetFactory, receiver);
        DrdsRunners.runHbtOnDrds(dataContext, substring, executorImplementor);
    }

    @NotNull
    private static LinkedList<SQLStatement> parse(String text) {
        text = text.trim();
        if (text.startsWith("begin") || text.startsWith("BEGIN")) {
            SQLStartTransactionStatement sqlStartTransactionStatement = new SQLStartTransactionStatement();
            LinkedList<SQLStatement> sqlStatements = new LinkedList<>();
            sqlStatements.add(sqlStartTransactionStatement);
            return sqlStatements;
        }
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(text, DbType.mysql, true);
        LinkedList<SQLStatement> statementList = new LinkedList<SQLStatement>();
        parser.parseStatementList(statementList, -1, null);
        return statementList;
    }

}