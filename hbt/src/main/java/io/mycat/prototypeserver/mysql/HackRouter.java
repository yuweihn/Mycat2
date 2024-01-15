/**
 * Copyright (C) <2021>  <chen junwen>
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
package io.mycat.prototypeserver.mysql;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.google.common.collect.ImmutableMap;
import io.mycat.*;
import io.mycat.calcite.rewriter.Distribution;
import io.mycat.calcite.rewriter.SQLRBORewriter;
import io.mycat.util.MycatSQLExprTableSourceUtil;
import io.mycat.util.NameMap;
import io.mycat.util.Pair;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class HackRouter {
    SQLStatement selectStatement;
    private String defaultSchema;
    Optional<Distribution> res;
    private MetadataManager metadataManager;
    private String targetName;
    private NameMap<Partition> targetMap;
    public static boolean PUSH_DOWN_SELECT_DUAL;
    public static boolean PUSH_SHOW;
    
    public HackRouter(SQLStatement selectStatement, MycatDataContext context) {
        this(selectStatement, context.getDefaultSchema());
    }

    public HackRouter(SQLStatement selectStatement, String defaultSchema) {
        this.selectStatement = selectStatement;
        this.defaultSchema = defaultSchema;
    }

    public boolean analyse() {
        Set<Pair<String, String>> tableNames = new HashSet<>();
        Set<String> methods = new HashSet<>();
        AtomicBoolean hasVar = new AtomicBoolean(false);
        selectStatement.accept(new MySqlASTVisitorAdapter() {
            @Override
            public boolean visit(SQLExprTableSource x) {
                String tableName = x.getTableName();
                if (tableName != null) {
                    String schema = Optional.ofNullable(x.getSchema()).orElse(defaultSchema);
                    if (schema == null) {
                        throw new MycatException("please use schema;");
                    }
                    tableNames.add(Pair.of(SQLUtils.normalize(schema), SQLUtils.normalize(tableName)));
                }
                return super.visit(x);
            }

            @Override
            public boolean visit(SQLMethodInvokeExpr x) {
                methods.add(x.getMethodName());
                return super.visit(x);
            }

            @Override
            public boolean visit(SQLVariantRefExpr x) {
                if (!"?".equals(x.getName())) {
                    hasVar.set(true);
                }
                return super.visit(x);
            }
        });
        this.metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        if (PUSH_DOWN_SELECT_DUAL && tableNames.isEmpty() && !hasVar.get()) {
            if (!methods.isEmpty() && methods.stream().noneMatch(name -> SQLRBORewriter.Information_Functions.containsKey(SQLUtils.normalize(name), false))) {
                targetMap = NameMap.immutableCopyOf(Collections.emptyMap());
                targetName = MetadataManager.getPrototype();
                return true;
            }
        }

        res = metadataManager.checkVaildNormalRoute(tableNames);
        if (res.isPresent()) {
            Distribution distribution = res.get();


            ImmutableMap.Builder<String, Partition> builder = ImmutableMap.builder();
            builder.putAll(
                    distribution.getGlobalTables().stream().distinct().collect(Collectors.toMap(k -> k.getUniqueName(), v -> v.getDataNode())));

            Map<String, Partition> normalMap = distribution.getNormalTables().stream().distinct().collect(Collectors.toMap(k -> k.getUniqueName(), v -> v.getDataNode()));
            builder.putAll(normalMap);


            targetMap = NameMap.immutableCopyOf(builder.build());
            switch (distribution.type()) {
                case BROADCAST:
                    List<Partition> globalDataNode = distribution.getGlobalTables().get(0).getGlobalDataNode();
                    int i = ThreadLocalRandom.current().nextInt(0, globalDataNode.size());
                    targetName = globalDataNode.get(i).getTargetName();
                    return true;
                case SHARDING:
                    return false;
                case PHY: {
                    targetName = normalMap.values().iterator().next().getTargetName();
                    return true;
                }
            }
        } else {
            return false;
        }
        return false;
    }

    public Pair<String, String> getPlan() {
        if (targetMap != null) {
            selectStatement.accept(new MySqlASTVisitorAdapter() {
                @Override
                public boolean visit(SQLExprTableSource x) {
                    String tableName = SQLUtils.normalize(x.getTableName());
                    String schema = SQLUtils.normalize(Optional.ofNullable(x.getSchema()).orElse(defaultSchema));
                    Partition partition = targetMap.get(schema + "_" + tableName, false);
                    if (partition != null) {
                        MycatSQLExprTableSourceUtil.setSqlExprTableSource(partition.getSchema(), partition.getTable(), x);
                    }
                    //with clause tmp table
                    return super.visit(x);
                }
            });
        }
        return Pair.of(targetName, selectStatement.toString());
    }
}
