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
package io.mycat.calcite.table;

import com.google.common.collect.ImmutableList;
import io.mycat.DataNode;
import io.mycat.SimpleColumnInfo;
import io.mycat.TableHandler;
import io.mycat.calcite.CalciteUtls;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.hbt3.AbstractMycatTable;
import io.mycat.hbt3.Distribution;
import io.mycat.hbt3.LazyRexDistribution;
import io.mycat.hbt4.ShardingInfo;
import io.mycat.metadata.GlobalTableHandler;
import io.mycat.metadata.NormalTableHandler;
import io.mycat.router.ShardingTableHandler;
import lombok.Getter;
import lombok.NonNull;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * @author Junwen Chen
 **/
@Getter
public class MycatLogicTable extends MycatTableBase implements AbstractMycatTable {
    final TableHandler table;
    final Statistic statistic;
    private static final Logger LOGGER = LoggerFactory.getLogger(MycatLogicTable.class);
    private final ShardingInfo shardingInfo;

    public MycatLogicTable(TableHandler t) {
        this.table = t;
        this.statistic = Statistics.createStatistic(table.getSchemaName(), table.getTableName(), table.getColumns());
        this.shardingInfo = ShardingInfo.create(t);
    }

    @Override
    public TableHandler logicTable() {
        return table;
    }

    @Override
    public Statistic getStatistic() {
        return statistic;
    }


    @Override
    public Distribution computeDataNode(List<RexNode> conditions) {
        switch (table.getType()) {
            case SHARDING:
                ShardingTableHandler shardingTableHandler = (ShardingTableHandler) this.table;
                return LazyRexDistribution.of(this, conditions, (paras) -> {
                    List<RexNode> rexNodes = new ArrayList<>();
                    for (RexNode condition : conditions) {
                        rexNodes.add(condition.accept(new RexShuttle() {
                            @Override
                            public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
                                RexBuilder rexBuilder = MycatCalciteSupport.INSTANCE.RexBuilder;
                                Object o = paras.get(dynamicParam.getIndex());
                                RelDataType type;
                                JavaTypeFactoryImpl typeFactory = MycatCalciteSupport.INSTANCE.TypeFactory;
                                if (o == null) {
                                    type = typeFactory.createSqlType(SqlTypeName.NULL);
                                } else {
                                    type = typeFactory.createJavaType(o.getClass());
                                }
                                return rexBuilder.makeLiteral(o, type, true);
                            }
                        }));
                    }
                    return CalciteUtls.getBackendTableInfos(shardingTableHandler, rexNodes);
                });
            case GLOBAL:
                return computeDataNode();
            case NORMAL:
                return computeDataNode();
        }
        throw new UnsupportedOperationException();
    }


    @Override
    public boolean isSingle(List<RexNode> conditions) {
        ShardingTableHandler shardingTableHandler = (ShardingTableHandler) this.table;
        int size = shardingTableHandler.dataNodes().size();
        if (size == 1) {
            return true;
        }
        List<SimpleColumnInfo> columns = shardingTableHandler.getColumns();
        if (conditions.size() == 1) {
            RexNode rexNode = conditions.get(0);
            if (rexNode.getKind() == SqlKind.EQUALS) {
                RexCall node = (RexCall) rexNode;
                List<RexNode> operands = node.getOperands();
                RexNode rexNode1 = CalciteUtls.unCastWrapper(operands.get(0));
                RexNode rexNode2 = CalciteUtls.unCastWrapper(operands.get(1));
                if (rexNode1 instanceof RexInputRef && (rexNode2 instanceof RexLiteral || rexNode2 instanceof RexDynamicParam)) {
                    int index = ((RexInputRef) rexNode1).getIndex();
                    @NonNull String columnName = columns.get(index).getColumnName();
                    return shardingTableHandler.function().isShardingKey(columnName);
                }
            }
        }
        return false;
    }

    public Distribution computeDataNode() {
        switch (table.getType()) {
            case SHARDING:
                ShardingTableHandler shardingTableHandler = (ShardingTableHandler) this.table;
                return Distribution.of(shardingTableHandler.dataNodes(), false, Distribution.Type.Sharding);
            case GLOBAL:
                GlobalTableHandler globalTableHandler = (GlobalTableHandler) this.table;
                List<DataNode> globalDataNode = globalTableHandler.getGlobalDataNode();
                int i = ThreadLocalRandom.current().nextInt(0, globalDataNode.size());
                return Distribution.of(ImmutableList.of(globalDataNode.get(i)), false, Distribution.Type.BroadCast);
            case NORMAL:
                DataNode dataNode = ((NormalTableHandler) table).getDataNode();
                return Distribution.of(ImmutableList.of(dataNode), false, Distribution.Type.PHY);
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public ShardingInfo getShardingInfo() {
        return shardingInfo;
    }

    @Override
    public boolean isPartial(List<RexNode> conditions) {
        ShardingTableHandler shardingTableHandler = (ShardingTableHandler) this.table;
        int size = shardingTableHandler.dataNodes().size();
        if (size > 1) {
            return isSingle(conditions);
        }
        return false;
    }

}