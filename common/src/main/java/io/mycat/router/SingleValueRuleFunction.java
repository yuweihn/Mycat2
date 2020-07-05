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
package io.mycat.router;

import io.mycat.DataNode;
import io.mycat.MycatException;
import io.mycat.RangeVariable;
import io.mycat.util.CollectionUtil;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * @author mycat
 * @author cjw
 * 路由算法接口
 */
public abstract class SingleValueRuleFunction extends CustomRuleFunction {

    public abstract String name();

    @Override
    public List<DataNode> calculate(Set<RangeVariable> values) {
        ArrayList<DataNode> res = new ArrayList<>();
        for (RangeVariable rangeVariable : values) {
            //匹配字段名
            if (getColumnName().equalsIgnoreCase(rangeVariable.getColumnName())) {
                ///////////////////////////////////////////////////////////////
                String begin = Objects.toString(rangeVariable.getBegin());
                String end = Objects.toString(rangeVariable.getEnd());
                switch (rangeVariable.getOperator()) {
                    case EQUAL: {
                        DataNode dataNode = this.calculate(begin);
                        if (dataNode != null) {
                            CollectionUtil.setOpAdd(res, dataNode);
                        } else {
                            return getTable().getShardingBackends();
                        }
                        break;
                    }
                    case RANGE: {
                        List<DataNode> dataNodes = this.calculateRange(begin, end);
                        if (dataNodes == null || dataNodes.size() == 0) {
                            return getTable().getShardingBackends();
                        }
                        CollectionUtil.setOpAdd(res, dataNodes);
                        break;
                    }
                }
            }
        }
        return res.isEmpty()?getTable().getShardingBackends():res;
    }

    public static int[] toIntArray(String string) {
        String[] strs = io.mycat.util.SplitUtil.split(string, ',', true);
        int[] ints = new int[strs.length];
        for (int i = 0; i < strs.length; ++i) {
            ints[i] = Integer.parseInt(strs[i]);
        }
        return ints;
    }

    /**
     * return matadata nodes's id columnValue is column's value
     *
     * @return never null
     */
    public abstract int calculateIndex(String columnValue);

    public abstract int[] calculateIndexRange(String beginValue, String endValue);


    /**
     * 对于存储数据按顺序存放的字段做范围路由，可以使用这个函数
     */
    public static int[] calculateSequenceRange(SingleValueRuleFunction algorithm, String beginValue,
                                               String endValue) {
        int begin = 0, end = 0;
        begin = algorithm.calculateIndex(beginValue);
        end = algorithm.calculateIndex(endValue);
        if (end >= begin) {
            int len = end - begin + 1;
            int[] re = new int[len];
            for (int i = 0; i < len; i++) {
                re[i] = begin + i;
            }
            return re;
        } else {
            return new int[0];
        }
    }

    public static int[] calculateAllRange(int count) {
        int[] ints = new int[count];
        for (int i = 0; i < count; i++) {
            ints[i] = i;
        }
        return ints;
    }

    protected static int[] ints(List<Integer> list) {
        int[] ints = new int[list.size()];
        for (int i = 0; i < ints.length; i++) {
            ints[i] = list.get(i);
        }
        return ints;
    }


    public DataNode calculate(String columnValue) {
        int i = calculateIndex(columnValue);
        if (i == -1) {
            return null;
        }
        ShardingTableHandler table = getTable();
        List<DataNode> shardingBackends = table.getShardingBackends();
        int size = shardingBackends.size();
        if (0 <= i && i < size) {
            return shardingBackends.get(i);
        } else {
            String message = MessageFormat.format("{0}.{1} 分片算法越界 {3} 分片值:{4}",
                    table.getSchemaName(), table.getTableName(), columnValue);
            throw new MycatException(message);
        }
    }


    public List<DataNode> calculateRange(String beginValue, String endValue) {
        int[] ints = calculateIndexRange(beginValue, endValue);
        ShardingTableHandler table = getTable();
        List<DataNode> shardingBackends = (List) table.getShardingBackends();
        int size = shardingBackends.size();
        if (ints == null) {
            return shardingBackends;
        }
        ArrayList<DataNode> res = new ArrayList<>();
        for (int i : ints) {
            if (0 <= i && i < size) {
                res.add(shardingBackends.get(i));
            } else {
                return shardingBackends;
            }
        }
        return res;
    }

}