package io.mycat.hbt4.executor;

import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLLimit;
import com.alibaba.fastsql.sql.ast.SQLOrderBy;
import com.alibaba.fastsql.sql.ast.SQLReplaceable;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.expr.SQLExprUtils;
import com.alibaba.fastsql.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectItem;
import com.alibaba.fastsql.sql.dialect.mysql.ast.clause.MySqlDeclareStatement;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlExportParameterVisitor;
import com.alibaba.fastsql.sql.visitor.VisitorFeature;
import com.google.common.collect.ImmutableList;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.JdbcRowBaseIterator;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.hbt4.Group;
import lombok.SneakyThrows;
import org.apache.calcite.sql.util.SqlString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class MycatPreparedStatementUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(MycatPreparedStatementUtil.class);

    public static void collect(SQLStatement sqlStatement, StringBuilder sb, List<Object> inputParameters, List<Object> outputParameters) {
        MySqlExportParameterVisitor parameterVisitor = new MySqlExportParameterVisitor(outputParameters, sb, true) {

            @Override
            public boolean visit(SQLOrderBy x) {
                try {
                    this.parameterized = false;
                    return super.visit(x);
                } finally {
                    this.parameterized = true;
                }
            }

            @Override
            public boolean visit(SQLLimit x) {
                try {
                    this.parameterized = false;
                    return super.visit(x);
                } finally {
                    this.parameterized = true;
                }
            }


            @Override
            public boolean visit(SQLSelectItem x) {
                try {
                    this.parameterized = false;
                    return super.visit(x);
                } finally {
                    this.parameterized = true;
                }
            }
        };
        parameterVisitor.setShardingSupport(false);
        parameterVisitor.setFeatures(VisitorFeature.OutputParameterizedQuesUnMergeInList.mask |
                VisitorFeature.OutputParameterizedQuesUnMergeAnd.mask |
                VisitorFeature.OutputParameterizedUnMergeShardingTable.mask |
                VisitorFeature.OutputParameterizedQuesUnMergeOr.mask
                | VisitorFeature.OutputParameterizedQuesUnMergeValuesList.mask
        );
        if (inputParameters != null) {
            parameterVisitor.setInputParameters(inputParameters);
        }

        sqlStatement.accept(parameterVisitor);
    }

    public static void outputToParameters(SQLStatement sqlStatement, StringBuilder sb, List<Object> outputParameters) {
        MySqlExportParameterVisitor parameterVisitor = new MySqlExportParameterVisitor(outputParameters, sb, true) {

        };
        parameterVisitor.setShardingSupport(false);
        parameterVisitor.setInputParameters(Collections.emptyList());
        sqlStatement.accept(parameterVisitor);
    }

    public static ExecuteBatchInsert batchInsert(String sql, Group value, Connection connection, String targetName) {
        if(LOGGER.isDebugEnabled()){
            for (List<Object> arg : value.getArgs()) {
                LOGGER.debug("batchInsert targetName:{} sql:{} parameters:{}",targetName,sql,arg);
            }
        }
        ExecuteBatchInsert executeBatchInsert = new ExecuteBatchInsert(sql, value, connection);
        return executeBatchInsert.invoke();
    }

    public static class ExecuteBatchInsert {
        private long lastInsertId;
        private long affected;
        private String sql;
        private Group value;
        private Connection connection;

        public ExecuteBatchInsert(String sql, Group value, Connection connection) {
            this.lastInsertId = 0;
            this.affected = 0;
            this.sql = sql;
            this.value = value;
            this.connection = connection;
        }

        public long getLastInsertId() {
            return lastInsertId;
        }

        public long getAffected() {
            return affected;
        }

        @SneakyThrows
        public ExecuteBatchInsert invoke() {
            try (PreparedStatement preparedStatement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
                for (List<Object> objects : value.args) {
                    setParams(preparedStatement, objects);
                    preparedStatement.addBatch();
                }
                int[] ints = preparedStatement.executeBatch();
                for (int anInt : ints) {
                    affected += anInt;
                }
                ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
                while (generatedKeys.next()) {
                    long aLong = generatedKeys.getLong(1);
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("batch parameterizedSql:{} args:{} lastInsertId:{}", sql, value.args, aLong);
                    }
                    lastInsertId = Math.max(lastInsertId, aLong);
                }
            }
            return this;
        }
    }

    public static void setParams(PreparedStatement preparedStatement, List<Object> objects) throws SQLException {
        int index = 1;
        for (Object object : objects) {
            preparedStatement.setObject(index, object);
            index++;
        }
    }

    public static String apply(String parameterizedSql, List<Object> parameters) {
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(parameterizedSql);
        sqlStatement.accept(new MySqlASTVisitorAdapter() {
            @Override
            public void endVisit(SQLVariantRefExpr x) {
                SQLReplaceable parent = (SQLReplaceable) x.getParent();
                parent.replace(x, SQLExprUtils.fromJavaObject(parameters.get(x.getIndex())));
            }
        });
        return sqlStatement.toString();
    }

    @SneakyThrows
    public static RowBaseIterator executeQuery(Connection mycatConnection,
                                               MycatRowMetaData calciteRowMetaData,
                                               SqlString value,
                                               List<Object> params) {
        String sql = value.getSql();
        PreparedStatement preparedStatement = mycatConnection.prepareStatement(sql);
        ImmutableList<Integer> dynamicParameters = value.getDynamicParameters();
        if (dynamicParameters != null && !dynamicParameters.isEmpty()) {
            MycatPreparedStatementUtil.setParams(preparedStatement, dynamicParameters.stream().map(i -> params.get(i)).collect(Collectors.toList()));
        }
        if (LOGGER.isDebugEnabled()){
            LOGGER.debug("sql:{} {}", sql, (params).toString());
        }
        try {
            ResultSet resultSet = preparedStatement.executeQuery();
            return new JdbcRowBaseIterator(calciteRowMetaData, preparedStatement, resultSet, null, sql);
        } catch (Throwable throwable) {
            LOGGER.error("sql:{} {}", sql, (params).toString(), throwable);
            throw throwable;
        }
    }

}