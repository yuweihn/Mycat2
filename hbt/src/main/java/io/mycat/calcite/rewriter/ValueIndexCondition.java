package io.mycat.calcite.rewriter;

import io.mycat.Partition;
import io.mycat.RangeVariable;
import io.mycat.RangeVariableType;
import io.mycat.SimpleColumnInfo.Type;
import io.mycat.calcite.table.ShardingTable;
import io.mycat.querycondition.ComparisonOperator;
import io.mycat.querycondition.QueryType;
import io.mycat.router.CustomRuleFunction;
import io.mycat.util.JsonUtil;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

import static io.mycat.querycondition.ComparisonOperator.LTE;

@AllArgsConstructor
@Getter
@ToString
public class ValueIndexCondition implements Comparable<ValueIndexCondition>, Serializable {
    List<String> fieldNames;
    String indexName;
    List<String> indexColumnNames;

    QueryType queryType;
    ComparisonOperator rangeQueryLowerOp;
    List<Object> rangeQueryLowerKey;
    ComparisonOperator rangeQueryUpperOp;
    List<Object> rangeQueryUpperKey;
    List<Object> pointQueryKey;

    public String toJson() {
        Map<String, String> map = new HashMap<>();
        map.put("fieldNames", JsonUtil.toJson(fieldNames));
        map.put("indexName", JsonUtil.toJson(indexName));
        map.put("indexColumnNames", JsonUtil.toJson(indexColumnNames));
        map.put("queryType", JsonUtil.toJson(queryType));
        map.put("rangeQueryLowerOp", JsonUtil.toJson(rangeQueryLowerOp));
        map.put("rangeQueryUpperOp", JsonUtil.toJson(rangeQueryUpperOp));
        map.put("rangeQueryLowerKey", JsonUtil.toJson(rangeQueryLowerKey));
        map.put("rangeQueryUpperKey", JsonUtil.toJson(rangeQueryUpperKey));
        map.put("pointQueryKey", JsonUtil.toJson(pointQueryKey));
        return JsonUtil.toJson(map);
    }

    public static ValueIndexCondition EMPTY = create(null, null, null);

    public ValueIndexCondition(List<String> fieldNames, String indexName, List<String> indexColumnNames) {
        if (fieldNames != null) {
            this.fieldNames = new ArrayList<>(fieldNames);
        }
        this.indexName = indexName;
        this.indexColumnNames = indexColumnNames;
    }


    public static ValueIndexCondition create(List<String> fieldNames, String indexName, List<String> indexColumnNames) {
        return new ValueIndexCondition(fieldNames, indexName, indexColumnNames);
    }

    @Override
    public int compareTo(@NotNull ValueIndexCondition o) {
        return this.queryType.compareTo(o.queryType);
    }

    public boolean canPushDown() {
        return queryType != null;
    }

    public String getName() {
        return indexName;
    }

    public ValueIndexCondition withQueryType(QueryType queryType) {
        this.queryType = queryType;
        return this;
    }

    public ValueIndexCondition withRangeQueryLowerOp(ComparisonOperator rangeQueryLowerOp) {
        this.rangeQueryLowerOp = rangeQueryLowerOp;
        return this;
    }

    public ValueIndexCondition withRangeQueryLowerKey(List<Object> rangeQueryLowerKey) {
        this.rangeQueryLowerKey = normalize(rangeQueryLowerKey);
        return this;
    }

    public ValueIndexCondition withRangeQueryUpperOp(ComparisonOperator op) {
        this.rangeQueryUpperOp = op;
        return this;
    }

    public ValueIndexCondition withRangeQueryUpperKey(List<Object> rangeQueryUpperKey) {
        this.rangeQueryUpperKey = normalize(rangeQueryUpperKey);
        return this;
    }

    public ValueIndexCondition withPointQueryKey(List<Object> pointQueryKey) {
        this.pointQueryKey = normalize(pointQueryKey);
        return this;
    }

    @NotNull
    private static List<Object> normalize(List<Object> pointQueryKey) {
        return pointQueryKey;
    }

    public QueryType getQueryType() {
        return queryType == null ? QueryType.PK_FULL_SCAN : queryType;
    }

    @NotNull
    public static List<Partition> getPartitions(ShardingTable shardingTable, CustomRuleFunction customRuleFunction, Map<QueryType, List<ValueIndexCondition>> conditions, List<Object> params) {
        if (conditions == null || conditions.isEmpty()) {
            return customRuleFunction.calculate(Collections.emptyMap());
        }
        Objects.requireNonNull(conditions);

        List<Partition> partitions = customRuleFunction.calculate(Collections.emptyMap());
        for (Map.Entry<QueryType, List<ValueIndexCondition>> entry : conditions.entrySet()) {
            for (ValueIndexCondition condition : entry.getValue()) {
                List<Object> pointQueryKey = resolveParam(params, condition.getPointQueryKey());
                ComparisonOperator rangeQueryLowerOp = condition.getRangeQueryLowerOp();
                List<Object> rangeQueryLowerKey = resolveParam(params, condition.getRangeQueryLowerKey());
                ComparisonOperator rangeQueryUpperOp = condition.getRangeQueryUpperOp();
                List<Object> rangeQueryUpperKey = resolveParam(params, condition.getRangeQueryUpperKey());
                switch (condition.getQueryType()) {
                    case PK_POINT_QUERY: {
                        //queryByPrimaryKey
                        Map<String, RangeVariable> map = new LinkedHashMap<>();// need column order

                        if (pointQueryKey.size() > 1) {
                            List<Partition> curPartitions = new LinkedList<>();
                            for (Object o1 : pointQueryKey) {
                                String indexColumnName = condition.getIndexColumnNames().get(0);
                                Type columnType = shardingTable.getLogicTable().getColumnByName(indexColumnName).getType();
                                RangeVariable rangeVariable = new RangeVariable(indexColumnName,columnType, RangeVariableType.EQUAL, o1);
                                map.put(indexColumnName, rangeVariable);
                                List<Partition> calculate = customRuleFunction.calculate(map);
                                curPartitions.addAll(calculate);
                            }
                            partitions = calculatePartitions(shardingTable, curPartitions, partitions);
                        } else {
                            Object o = pointQueryKey.get(0);
                            String indexColumnName = condition.getIndexColumnNames().get(0);
                            Type columnType = shardingTable.getLogicTable().getColumnByName(indexColumnName).getType();
                            RangeVariable rangeVariable = new RangeVariable(indexColumnName, columnType, RangeVariableType.EQUAL, o);
                            map.put(indexColumnName, rangeVariable);
                            partitions = calculatePartitions(shardingTable, customRuleFunction, map, partitions);
                        }
                        break;
                    }
                    case PK_RANGE_QUERY: {
                        ArrayList<Object> leftValues = new ArrayList<>(2);
                        ArrayList<Object> rightValues = new ArrayList<>(2);

                        if (rangeQueryUpperOp == ComparisonOperator.LT || rangeQueryUpperOp == LTE) {
                            for (Object o1 : rangeQueryLowerKey) {
                                if (o1 instanceof RexNode) {
                                    o1 = MycatRexUtil.resolveParam((RexNode) o1, params);
                                }
                                leftValues.add(o1);
                            }
                            Collections.sort((List) rangeQueryLowerKey);
                        }
                        if (rangeQueryLowerOp == ComparisonOperator.GT || rangeQueryLowerOp == ComparisonOperator.GTE) {
                            for (Object o1 : rangeQueryUpperKey) {
                                if (o1 instanceof RexNode) {
                                    o1 = MycatRexUtil.resolveParam((RexNode) o1, params);
                                }
                                rightValues.add(o1);
                            }

                            Collections.sort((List) rangeQueryUpperKey);
                        }
                        Object smallOne = null;
                        Object bigOne = null;
                        if (!rangeQueryLowerKey.isEmpty()) {
                            smallOne = rangeQueryLowerKey.get(0);
                        }
                        if (!rangeQueryUpperKey.isEmpty()) {
                            bigOne = rangeQueryUpperKey.get(rangeQueryUpperKey.size() - 1);
                        }
                        Map<String, RangeVariable> map = new HashMap<>();
                        if (smallOne != null && bigOne != null) {
                            if (rangeQueryUpperOp  == ComparisonOperator.LT){
                                rangeQueryUpperOp = LTE;
                            }
                            if (rangeQueryLowerOp  == ComparisonOperator.GT){
                                rangeQueryLowerOp = ComparisonOperator.GTE;
                            }
                            if (rangeQueryUpperOp == LTE && rangeQueryLowerOp == ComparisonOperator.GTE ||
                                    rangeQueryUpperOp == ComparisonOperator.LT && rangeQueryLowerOp == ComparisonOperator.GT)
                                for (String indexColumnName : condition.getIndexColumnNames()) {
                                    Type columnType = shardingTable.getLogicTable()
                                        .getColumnByName(indexColumnName).getType();
                                    RangeVariable rangeVariable = new RangeVariable(indexColumnName, columnType, RangeVariableType.RANGE, smallOne, bigOne);
                                    map.put(indexColumnName, rangeVariable);
                                }
                        } else if (smallOne != null) {
                            RangeVariableType type = null;
                            if (rangeQueryUpperOp == ComparisonOperator.LT) {
                                type = RangeVariableType.LT;
                            } else if (rangeQueryUpperOp == LTE) {
                                type = RangeVariableType.LTE;
                            }
                            if (type != null) {
                                for (String indexColumnName : condition.getIndexColumnNames()) {
                                    Type columnType = shardingTable.getLogicTable().getColumnByName(indexColumnName).getType();
                                    RangeVariable rangeVariable = new RangeVariable(indexColumnName, columnType, type, smallOne);
                                    map.put(indexColumnName, rangeVariable);
                                }
                            }
                        } else if (bigOne != null) {
                            RangeVariableType type = null;
                            if (rangeQueryUpperOp == ComparisonOperator.GT) {
                                type = RangeVariableType.GT;
                            } else if (rangeQueryUpperOp == ComparisonOperator.GTE) {
                                type = RangeVariableType.GTE;
                            }
                            if (type != null) {
                                for (String indexColumnName : condition.getIndexColumnNames()) {
                                    Type columnType = shardingTable.getLogicTable().getColumnByName(indexColumnName).getType();
                                    RangeVariable rangeVariable = new RangeVariable(indexColumnName, columnType, type, bigOne);
                                    map.put(indexColumnName, rangeVariable);
                                }
                            }
                        }
                        partitions = calculatePartitions(shardingTable, customRuleFunction, map, partitions);
                        break;
                    }
                    case PK_FULL_SCAN:
                        break;
                    default:

                }
            }
        }
        return partitions;
    }

    private static List<Partition> calculatePartitions(ShardingTable shardingTable, CustomRuleFunction customRuleFunction, Map<String, RangeVariable> map, List<Partition> partitions) {

        List<Partition> curPartitions = customRuleFunction.calculate(map);
        return calculatePartitions(shardingTable, partitions, curPartitions);
    }

    private static List<Partition> calculatePartitions(ShardingTable shardingTable, List<Partition> partitions, List<Partition> curPartitions) {
        if (partitions == null) {
            partitions = curPartitions;
        } else {
            partitions = intersection(partitions, curPartitions);
        }
        return partitions;
    }

    public static List intersection(List list1, List list2) {
        Objects.requireNonNull(list1);
        Objects.requireNonNull(list2);

        ArrayList result = new ArrayList();
        Iterator iterator = list2.iterator();

        while (iterator.hasNext()) {
            Object o = iterator.next();
            if (list1.contains(o)) {
                result.add(o);
            }
        }

        return result;
    }


    public static List<Object> resolveParam(List<Object> params, List<Object> pointQueryKeyArg) {
        if (pointQueryKeyArg == null) return Collections.emptyList();
        ArrayList<Object> builder = new ArrayList<>();
        for (Object o : pointQueryKeyArg) {
            if (o instanceof RexDynamicParam) {
                o = (params.get(((RexDynamicParam) o).getIndex()));
            }
            if (o instanceof RexCall && ((RexCall) o).getKind() == SqlKind.CAST) {
                o = ((RexCall) o).getOperands().get(0);
            }
            if (o instanceof NlsString) {
                o = ((NlsString) o).getValue();
            }
            if (o instanceof DateString) {
                o = ((DateString) o).toString();
            }
            if (o instanceof TimeString) {
                o = ((TimeString) o).toString();
            }
            if (o instanceof TimestampString) {
                o = ((TimestampString) o).toString();
            }
            if (o instanceof ByteString) {
                o = ((ByteString) o).getBytes();
            }
            if (o instanceof RexLiteral) {
                RexLiteral rexLiteral = (RexLiteral) o;
                RelDataType type = rexLiteral.getType();
                switch (type.getSqlTypeName()) {
                    case BOOLEAN:
                        o = rexLiteral.getValueAs(Boolean.class);
                        break;
                    case INTEGER:
                    case SMALLINT:
                    case TINYINT:
                    case BIGINT:
                        o = rexLiteral.getValueAs(Long.class);
                        break;
                    case DECIMAL:
                        o = rexLiteral.getValueAs(BigDecimal.class);
                        break;
                    case FLOAT:
                    case REAL:
                    case DOUBLE:
                        o = rexLiteral.getValueAs(Double.class);
                        break;
                    case DATE:
                        o = rexLiteral.getValueAs(LocalDate.class);
                        break;
                    case TIMESTAMP:
                    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                        o = rexLiteral.getValueAs(LocalDateTime.class);
                        break;
                    case TIME:
                    case TIME_WITH_LOCAL_TIME_ZONE:
                    case CHAR:
                    case VARCHAR:
                    case BINARY:
                    case VARBINARY:
                    case NULL:
                    case ANY:
                        ;
                    case SYMBOL:
                    case MULTISET:
                    case ARRAY:
                    case MAP:
                    case DISTINCT:
                    case STRUCTURED:
                    case ROW:
                    case OTHER:
                    case CURSOR:
                    case COLUMN_LIST:
                    case DYNAMIC_STAR:
                    case GEOMETRY:
                    case SARG:
                        o = rexLiteral.getValueAs(String.class);
                        break;
                }
            }
            if (o instanceof RexCall) {
                //Skip if it cannot become literal
                continue;
            }
            builder.add(o);
        }

        return builder;
    }


}
