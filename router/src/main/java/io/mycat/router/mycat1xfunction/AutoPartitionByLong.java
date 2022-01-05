/**
 * Copyright (C) <2021>  <mycat>
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
package io.mycat.router.mycat1xfunction;

import io.mycat.router.CustomRuleFunction;
import io.mycat.router.Mycat1xSingleValueRuleFunction;
import io.mycat.router.NodeIndexRange;
import io.mycat.router.ShardingTableHandler;

import java.util.List;
import java.util.Map;

public class AutoPartitionByLong extends Mycat1xSingleValueRuleFunction {

    private List<NodeIndexRange> longRanges;
    private int defaultNode = -1;
    private int partitionCount;

    @Override
    public String name() {
        return "AutoPartitionByLong";
    }

    @Override
    public void init(ShardingTableHandler tableHandler, Map<String, Object> prot, Map<String, Object> ranges) {
        this.defaultNode = Integer.parseInt(prot.get("defaultNode").toString());
        this.longRanges = NodeIndexRange.getLongRanges(getRangeFromPropertyOrRangeConfig(AutoPartitionByLong.class,prot,ranges));
        this.partitionCount = NodeIndexRange.getPartitionCount(this.longRanges);
    }


    @Override
    public int calculateIndex(String columnValue) {
        try {
            long value = Long.parseLong(columnValue);
            for (NodeIndexRange longRang : this.longRanges) {
                if (value <= longRang.valueEnd && value >= longRang.valueStart) {
                    return longRang.nodeIndex;
                }
            }
            return defaultNode;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "columnValue:" + columnValue + " Please eliminate any quote and non number within it.",
                    e);
        }
    }

    @Override
    public int[] calculateIndexRange(String beginValue, String endValue) {
        return calculateSequenceRange(this, beginValue, endValue);
    }

    @Override
    public boolean isSameDistribution(CustomRuleFunction customRuleFunction) {
        if (customRuleFunction == null) return false;
        if (customRuleFunction.getClass() == AutoPartitionByLong.class) {

            AutoPartitionByLong customRuleFunction1 = (AutoPartitionByLong) customRuleFunction;
            List<NodeIndexRange> longRanges = customRuleFunction1.longRanges;
            int defaultNode = customRuleFunction1.defaultNode;
            int partitionCount = customRuleFunction1.partitionCount;

            return this.longRanges.equals(longRanges)
                    &&
                    this.defaultNode == defaultNode
                    &&
                    this.partitionCount == partitionCount;

        }
        return false;
    }

    @Override
    public String getErUniqueID() {
        return getClass().getName()+":"+longRanges+defaultNode+partitionCount;
    }
}