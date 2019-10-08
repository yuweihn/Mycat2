package io.mycat.calcite;

import com.alibaba.fastsql.DbType;
import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;

import java.util.Map;

public class MysqlTableReplacer extends MySqlASTVisitorAdapter {
    private final Map<String, MetadataManager.SchemaInfo> dbSet;
    private final String schemaName;

    public MysqlTableReplacer(Map<String, MetadataManager.SchemaInfo> dbSet, String schemaName) {
        this.dbSet = dbSet;
        this.schemaName = schemaName;
    }

    @Override
    public boolean visit(SQLExprTableSource x) {
        String schemaName = x.getSchema();
        String tableName = x.getTableName();
        if (schemaName != null) {
            schemaName = SQLUtils.forcedNormalize(schemaName, DbType.mysql);
        }
        if (tableName != null) {
            tableName = SQLUtils.forcedNormalize(tableName, DbType.mysql);
        }
        if (schemaName == null) {
            schemaName = this.schemaName;
        }
        MetadataManager.SchemaInfo mappingTable = getMappingTable(schemaName, tableName);
        if (mappingTable!=null){
            x.setExpr(new SQLPropertyExpr(mappingTable.targetSchema, mappingTable.targetTable));
        }
        return super.visit(x);
    }


    public MetadataManager.SchemaInfo getMappingTable(String schemaName, String tableName) {
        String key = schemaName.toLowerCase() + "." + tableName.toLowerCase();
        return dbSet.get(key);
    }

}