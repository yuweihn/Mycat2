/**
 * Copyright (C) <2019>  <chen junwen>
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
package io.mycat.calcite;

import io.mycat.calcite.shardingQuery.SchemaInfo;
import io.mycat.router.RuleFunction;
import io.mycat.sqlparser.util.complie.RangeVariableType;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Weiqing Xu
 * @author Junwen Chen
 **/
public class DataMappingEvaluator {
    private final Map<String, SortedSet<RangeVariable>> columnMap = new HashMap<>();

    void assignment(boolean or, String columnName, String value) {
        getRangeVariables(columnName).add(new RangeVariable(or, RangeVariableType.EQUAL, value));
    }

    void assignmentRange(boolean or, String columnName, String begin, String end) {
        getRangeVariables(columnName).add(new RangeVariable(or, RangeVariableType.RANGE, begin, end));
    }

    private SortedSet<RangeVariable> getRangeVariables(String columnName) {
        return columnMap.computeIfAbsent(columnName, s -> new TreeSet<>());
    }

    public List<BackendTableInfo> calculate(MetadataManager.LogicTable logicTable) {
        if (logicTable.getNatureTableColumnInfo() != null) {
            return getBackendTableInfosByNatureDatabaseTable(logicTable);
        } else {
            return getBackendTableInfosByMap(logicTable);
        }
    }

    private List<BackendTableInfo> getBackendTableInfosByMap(MetadataManager.LogicTable logicTable) {
        List<String> targetSet = Collections.emptyList();
        List<String> databaseSet = Collections.emptyList();
        List<String> tableSet = Collections.emptyList();
        if (logicTable.getReplicaColumnInfo() != null) {
            targetSet = getRouteColumnSortedSet(logicTable.getReplicaColumnInfo());
        }
        if (logicTable.getDatabaseColumnInfo() != null) {
            databaseSet = getRouteColumnSortedSet(logicTable.getDatabaseColumnInfo());
        }
        if (logicTable.getTableColumnInfo() != null) {
            tableSet = getRouteColumnSortedSet(logicTable.getTableColumnInfo());
        }

        List<BackendTableInfo> res = new ArrayList<>();

        for (String targetName : targetSet) {
            for (String databaseName : databaseSet) {
                for (String tableName : tableSet) {
                    res.add(new BackendTableInfo(targetName, new SchemaInfo(databaseName, tableName)));
                }
            }
        }
        return res.isEmpty()?logicTable.backends:res;
    }

    private List<BackendTableInfo> getBackendTableInfosByNatureDatabaseTable(MetadataManager.LogicTable logicTable) {
        List<Integer> routeIndexSortedSet = getRouteIndexSortedSet(logicTable.getNatureTableColumnInfo());
        if (routeIndexSortedSet.isEmpty()) {
            return logicTable.backends;
        } else {
            return routeIndexSortedSet.stream().map(logicTable.backends::get).collect(Collectors.toList());
        }
    }

    private List<String> getRouteColumnSortedSet(SimpleColumnInfo.ShardingInfo target) {
        return getRouteIndexSortedSet(target).stream().map(i -> target.map.get(i)).collect(Collectors.toList());
    }

    private List<Integer> getRouteIndexSortedSet(SimpleColumnInfo.ShardingInfo target) {
        SortedSet<RangeVariable> rangeVariables = columnMap.get(target.columnInfo.columnName);
        if (rangeVariables == null) {
            throw new UnsupportedOperationException();
        } else {
            return calculate(target.getFunction(), rangeVariables).stream().sorted().collect(Collectors.toList());
        }
    }

    private Set<Integer> calculate(RuleFunction ruleFunction, SortedSet<RangeVariable> value) {
        HashSet<Integer> res = new HashSet<>();
        for (RangeVariable rangeVariable : value) {
            String begin = Objects.toString(rangeVariable.getBegin());
            String end = Objects.toString(rangeVariable.getEnd());
            switch (rangeVariable.getOperator()) {
                case EQUAL: {
                    int calculate = ruleFunction.calculate(begin);
                    if (calculate == -1) {
                        return Collections.emptySet();
                    }
                    res.add(calculate);
                    break;
                }
                case RANGE: {
                    int[] calculate = ruleFunction.calculateRange(begin, end);
                    if (calculate == null || calculate.length == 0) {
                        return Collections.emptySet();
                    }
                    for (int i : calculate) {
                        if (i == -1) {
                            return Collections.emptySet();
                        }
                        res.add(i);
                    }
                    break;
                }
            }
        }
        return res;
    }
//
//    public String getFilterExpr(List<String> rowOrder) {
//        StringBuilder where = new StringBuilder();
//        for (String columnName : rowOrder) {
//            SortedSet<RangeVariable> value = columnMap.get(columnName);
//            for (RangeVariable rangeVariable : value) {
//                if (where.length() > 0) {
//                    if (rangeVariable.isOr()) {
//                        where.append(" or (");
//                    } else {
//                        where.append(" and (");
//                    }
//                } else {
//                    where.append("  (");
//                }
//                switch (rangeVariable.getOperator()) {
//                    case EQUAL:
//                        where.append(columnName).append(" = ").append(rangeVariable.getBegin());
//                        break;
//                    case RANGE:
//                        where.append(columnName).append(" between ").append(rangeVariable.getBegin()).append(" and ").append(rangeVariable.getEnd());
//                        break;
//                }
//                where.append(" ) ");
//            }
//        }
//        return where.toString();
//    }

    public void merge(DataMappingEvaluator arg) {
        arg.columnMap.forEach((key, value) -> this.getRangeVariables(key).addAll(value));
    }
}