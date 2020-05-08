package io.mycat.util;

import com.alibaba.fastsql.sql.ast.SQLStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface SQLDispatcher {

    final static Logger logger = LoggerFactory.getLogger(SQLDispatcher.class);

//    default public void parse(String sql, Response receiver) {
//        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, false);
//        LinkedList<SQLStatement> statementList = new LinkedList<SQLStatement>();
//        parser.parseStatementList(statementList, -1, null);
//        Iterator<SQLStatement> iterator = statementList.iterator();
//        while (iterator.hasNext()) {
//            SQLStatement statement = preHandle(iterator.next());
//            receiver.setHasMore(iterator.hasNext());
//            try {
//                handleStatement(statement, receiver);
//            } catch (Throwable e) {
//                boolean isRun = false;
//                try {
//                    tryHandleHbt(sql);
//                    isRun = true;
//                } catch (Throwable e1) {
//                    logger.error("",e1);
//                }finally {
//                    if (!isRun) {
//                        receiver.sendError(e);
//                    }
//                }
//                return;
//            } finally {
//                iterator.remove();//help gc
//            }
//        }
//    }

//    void tryHandleHbt(String sql);
//
//    default void handleStatement(SQLStatement statement, Response receiver) {
//        if (statement instanceof SQLSelectStatement) {
//            handleSelect((SQLSelectStatement) statement, receiver);
//        } else if (statement instanceof MySqlInsertStatement) {
//            handleInsert((MySqlInsertStatement) statement, receiver);
//        } else if (statement instanceof SQLReplaceStatement) {
//            handleReplace((SQLReplaceStatement) statement, receiver);
//        } else if (statement instanceof MySqlDeleteStatement) {
//            handleDelete((MySqlDeleteStatement) statement, receiver);
//        } else if (statement instanceof MySqlUpdateStatement) {
//            handleUpdate((MySqlUpdateStatement) statement, receiver);
//        } else if (statement instanceof MySqlLoadDataInFileStatement) {
//            handleLoaddata((MySqlLoadDataInFileStatement) statement, receiver);
//        } else if (statement instanceof SQLTruncateStatement) {
//            handleTruncate((SQLTruncateStatement) statement, receiver);
//        } else if (statement instanceof SQLSetStatement) {
//            handleSet((SQLSetStatement) statement, receiver);
//        } else if (statement instanceof MySqlSetTransactionStatement) {
//            handleSetTransaction((MySqlSetTransactionStatement) statement, receiver);
//        } else if (statement instanceof SQLUseStatement) {
//            handleUse((SQLUseStatement) statement, receiver);
//        } else if (statement instanceof SQLCommitStatement) {
//            handleCommit((SQLCommitStatement) statement, receiver);
//        } else if (statement instanceof SQLRollbackStatement) {
//            handleRollback((SQLRollbackStatement) statement, receiver);
//        } else if (statement instanceof SQLStartTransactionStatement) {
//            handleSQLStartTransaction((SQLStartTransactionStatement) statement, receiver);
//        } else if (statement instanceof MySqlKillStatement) {
//            handleKill((MySqlKillStatement) statement, receiver);
//        } else if (statement instanceof MySqlExplainStatement) {
//            handleExplain((MySqlExplainStatement) statement, receiver);
//        } else if (statement instanceof SQLAlterDatabaseStatement) {
//            handleAlterDatabase((SQLAlterDatabaseStatement) statement, receiver);
//        } else if (statement instanceof SQLAlterTableStatement) {
//            handleAlterTable((SQLAlterTableStatement) statement, receiver);
//        } else if (statement instanceof SQLCreateDatabaseStatement) {
//            handleCreateDatabaseStatement((SQLCreateDatabaseStatement) statement, receiver);
//        } else if (statement instanceof SQLCreateIndexStatement) {
//            handleCreateIndex((SQLCreateIndexStatement) statement, receiver);
//        } else if (statement instanceof SQLCreateTableStatement) {
//            handleCreateTable((SQLCreateTableStatement) statement, receiver);
//        } else if (statement instanceof SQLCreateViewStatement) {
//            handleCreateView((SQLCreateViewStatement) statement, receiver);
//        } else if (statement instanceof SQLDropDatabaseStatement) {
//            handleDropDatabaseStatement((SQLDropDatabaseStatement) statement, receiver);
//        } else if (statement instanceof SQLDropTableStatement) {
//            handleDropTableStatement((SQLDropTableStatement) statement, receiver);
//        } else if (statement instanceof SQLDropViewStatement) {
//            handleDropViewStatement((SQLDropViewStatement) statement, receiver);
//        } else if (statement instanceof MySqlRenameTableStatement) {
//            handleMySqlRenameTable((MySqlRenameTableStatement) statement, receiver);
//        } else if (statement instanceof SQLShowTablesStatement) {
//            handleShowTables((SQLShowTablesStatement) statement, receiver);
//        } else if (statement instanceof MySqlShowTableStatusStatement) {
//            handleShowTableStatus((MySqlShowTableStatusStatement) statement, receiver);
//        } else if (statement instanceof MySqlShowVariantsStatement) {
//            handleMySqlShowVariants((MySqlShowVariantsStatement) statement, receiver);
//        } else if (statement instanceof MySqlShowWarningsStatement) {
//            handleMySqlShowWarnings((MySqlShowWarningsStatement) statement, receiver);
//        } else if (statement instanceof MySqlShowProcessListStatement) {
//            handleMySqlShowProcessList((MySqlShowProcessListStatement) statement, receiver);
//        } else if (statement instanceof SQLShowIndexesStatement) {
//            handleShowIndexes((SQLShowIndexesStatement) statement, receiver);
//        } else if (statement instanceof SQLShowColumnsStatement) {
//            handleMySqlShowColumns((SQLShowColumnsStatement) statement, receiver);
//        } else if (statement instanceof MySqlShowErrorsStatement) {
//            handleMySqlShowErrors((MySqlShowErrorsStatement) statement, receiver);
//        } else if (statement instanceof MySqlShowEnginesStatement) {
//            handleMySqlShowEngines((MySqlShowEnginesStatement) statement, receiver);
//        } else if (statement instanceof SQLShowCreateTableStatement) {
//            handleMySqlShowCreateTable((SQLShowCreateTableStatement) statement, receiver);
//        } else if (statement instanceof MySqlShowCollationStatement) {
//            handleMySqlShowCollation((MySqlShowCollationStatement) statement, receiver);
//        } else if (statement instanceof MySqlShowCharacterSetStatement) {
//            handleMySqlShowCharacterSet((MySqlShowCharacterSetStatement) statement, receiver);
//        } else if (statement instanceof MySqlShowDatabaseStatusStatement) {
//            handleMySqlShowDatabaseStatus((MySqlShowDatabaseStatusStatement) statement, receiver);
//        } else if (statement instanceof MySqlHintStatement) {
//            MySqlHintStatement statement1 = (MySqlHintStatement) statement;
//            handleMySqlHintStatement(statement1, receiver);
//        } else if (statement instanceof com.alibaba.fastsql.sql.ast.statement.SQLShowDatabasesStatement) {
//            handleSQLShowDatabasesStatement((com.alibaba.fastsql.sql.ast.statement.SQLShowDatabasesStatement) statement, receiver);
//        } else {
//            logger.warn(" ---------statement:" + statement.getClass());
//            //  throw new UnsupportedOperationException("UnsupportedStatement:" + statement.getClass().getSimpleName());
//        }
//    }

//    void handleSQLShowDatabasesStatement(SQLShowDatabasesStatement statement, Response receiver);
//
//    void handleMySqlHintStatement(MySqlHintStatement statement1, Response receiver);

    public abstract SQLStatement preHandle(SQLStatement statement);

    public static void main(String[] args) {

    }
//
//    public abstract void handleMySqlShowCharacterSet(MySqlShowCharacterSetStatement statement, Response receiver);
//
//    public abstract void handleMySqlShowEngines(MySqlShowEnginesStatement statement, Response receiver);
//
//    public abstract void handleMySqlShowCollation(MySqlShowCollationStatement statement, Response receiver);
//
//    public abstract void handleMySqlShowCreateTable(SQLShowCreateTableStatement statement, Response receiver);
//
//    public abstract void handleMySqlShowDatabaseStatus(MySqlShowDatabaseStatusStatement statement, Response receiver);
//
//    public abstract void handleMySqlShowErrors(MySqlShowErrorsStatement statement, Response receiver);
//
//    public abstract void handleMySqlShowColumns(SQLShowColumnsStatement statement, Response receiver);
//
//    public abstract void handleShowIndexes(SQLShowIndexesStatement statement, Response receiver);
//
//    public abstract void handleMySqlShowProcessList(MySqlShowProcessListStatement statement, Response receiver);
//
//    public abstract void handleMySqlShowWarnings(MySqlShowWarningsStatement statement, Response receiver);
//
//    public abstract void handleMySqlShowVariants(MySqlShowVariantsStatement statement, Response receiver);
//
//    public abstract void handleShowTableStatus(MySqlShowTableStatusStatement statement, Response receiver);
//
//    public abstract void handleShowTables(SQLShowTablesStatement statement, Response receiver);
//
//    public abstract void handleMySqlRenameTable(MySqlRenameTableStatement statement, Response receiver);
//
//    public abstract void handleDropViewStatement(SQLDropViewStatement statement, Response receiver);
//
//    public abstract void handleDropTableStatement(SQLDropTableStatement statement, Response receiver);
//
//
//    public abstract void handleDropDatabaseStatement(SQLDropDatabaseStatement statement, Response receiver);
//
//    public abstract void handleCreateView(SQLCreateViewStatement statement, Response receiver);
//
//    public abstract void handleCreateTable(SQLCreateTableStatement statement, Response receiver);
//
//    public abstract void handleCreateIndex(SQLCreateIndexStatement statement, Response receiver);
//
//    public abstract void handleCreateDatabaseStatement(SQLCreateDatabaseStatement statement, Response receiver);
//
//    public abstract void handleAlterTable(SQLAlterTableStatement statement, Response receiver);
//
//    public abstract void handleAlterDatabase(SQLAlterDatabaseStatement statement, Response receiver);
//
//    public abstract void handleExplain(MySqlExplainStatement statement, Response receiver);
//
//    public abstract void handleKill(com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlKillStatement statement, Response receiver);
//
//    public abstract void handleSQLStartTransaction(SQLStartTransactionStatement statement, Response receiver);
//
//    public abstract void handleRollback(SQLRollbackStatement statement, Response receiver);
//
//    public abstract void handleCommit(SQLCommitStatement statement, Response receiver);
//
//    public abstract void handleUse(SQLUseStatement statement, Response receiver);
//
//    public abstract void handleSetTransaction(MySqlSetTransactionStatement statement, Response receiver);
//
//    public abstract void handleSet(SQLSetStatement statement, Response receiver);
//
//    public abstract void handleTruncate(SQLTruncateStatement statement, Response receiver);
//
//    public abstract void handleLoaddata(com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlLoadDataInFileStatement statement, Response receiver);
//
//    public abstract void handleUpdate(MySqlUpdateStatement statement, Response receiver);
//
//    public abstract void handleDelete(MySqlDeleteStatement statement, Response receiver);
//
//    public abstract void handleReplace(SQLReplaceStatement statement, Response receiver);
//
//    public abstract void handleInsert(MySqlInsertStatement statement, Response receiver);
//
//    public abstract void handleSelect(SQLSelectStatement statement, Response receiver);

}