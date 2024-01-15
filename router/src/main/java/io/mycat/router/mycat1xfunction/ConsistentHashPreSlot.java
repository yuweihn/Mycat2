package io.mycat.router.mycat1xfunction;

import io.mycat.router.CustomRuleFunction;
import io.mycat.router.Mycat1xSingleValueRuleFunction;
import io.mycat.router.NodeIndexRange;
import io.mycat.router.ShardingTableHandler;
import io.mycat.router.hashfunction.HashFunction;
import io.mycat.router.migrate.ConsistentHashBalanceExpandResult;
import io.mycat.router.migrate.MigrateTask;
import io.mycat.router.migrate.MigrateUtils;

import java.util.*;

/**
 * jamie12221
 */
public class ConsistentHashPreSlot extends Mycat1xSingleValueRuleFunction {

    public ConsistentHashPreSlot(String name, int defaultSlotsNum, HashFunction hashFunction) {
        this.name = name;
        this.DEFAULT_SLOTS_NUM = defaultSlotsNum;
        this.rangeMap2 = new int[defaultSlotsNum];
        this.hashFunction = hashFunction;
    }

    private final String name;
    private final int DEFAULT_SLOTS_NUM;
    private final int[] rangeMap2;
    private final HashFunction hashFunction;
    private List<List<NodeIndexRange>> longRanges;


    @Override
    public String name() {
        return name;
    }

    @Override
    public int calculateIndex(String columnValue) {
        long hash = hashFunction.hash(columnValue);
        int slot = (int) (hash % DEFAULT_SLOTS_NUM);
        return rangeMap2[slot];
    }

    @Override
    public int[] calculateIndexRange(String beginValue, String endValue) {
        return null;
    }

    public ConsistentHashBalanceExpandResult balanceExpand(ShardingTableHandler table, List<String> oldDataNodes, List<String> newDataNodes) {
        List<List<NodeIndexRange>> copy = MigrateUtils.copy(longRanges);
        SortedMap<String, List<MigrateTask>> stringListSortedMap = MigrateUtils.balanceExpand(copy, oldDataNodes, newDataNodes, DEFAULT_SLOTS_NUM);
        MigrateUtils.merge(copy, stringListSortedMap);
        ConsistentHashPreSlot consistentHash = new ConsistentHashPreSlot(name, DEFAULT_SLOTS_NUM, hashFunction);
        consistentHash.init(table, Collections.emptyMap(), (Map) NodeIndexRange.from(copy));
        return new ConsistentHashBalanceExpandResult(stringListSortedMap, consistentHash);
    }


    @Override
    protected void init(ShardingTableHandler table, Map<String, Object> prot, Map<String, Object> ranges) {
        this.table = table;
        this.properties = prot;
        this.ranges = ranges;
        String countText = prot.get("count").toString();
        if (countText != null) {
            int count = Integer.parseInt(countText);
            int slotSize = DEFAULT_SLOTS_NUM / count;
            longRanges = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                if (i == count - 1) {
                    longRanges.add(new ArrayList<>(Collections.singletonList(new NodeIndexRange(i, i * slotSize, (DEFAULT_SLOTS_NUM - 1)))));
                } else {
                    longRanges.add(new ArrayList<>(Collections.singletonList(new NodeIndexRange(i, i * slotSize, ((i + 1) * slotSize - 1)))));
                }
            }
        } else {
            longRanges = NodeIndexRange.getSplitLongRanges(ranges);
        }
        for (List<NodeIndexRange> longRanges : longRanges) {
            for (NodeIndexRange longRange : longRanges) {
                int valueStart = (int) longRange.valueStart;
                int valueEnd = (int) longRange.valueEnd;
                int nodeIndex = longRange.nodeIndex;
                for (int i = valueStart; i <= valueEnd; i++) {
                    rangeMap2[i] = nodeIndex;
                }
            }
        }
    }

    @Override
    public boolean isSameDistribution(CustomRuleFunction customRuleFunction) {
        if (customRuleFunction == null) return false;
        if (ConsistentHashPreSlot.class.isAssignableFrom(customRuleFunction.getClass())) {
            ConsistentHashPreSlot customRuleFunction1 = (ConsistentHashPreSlot) customRuleFunction;
            final int DEFAULT_SLOTS_NUM = customRuleFunction1.DEFAULT_SLOTS_NUM;
            final int[] rangeMap2 = customRuleFunction1.rangeMap2;
            final HashFunction hashFunction = customRuleFunction1.hashFunction;
            List<List<NodeIndexRange>> longRanges = customRuleFunction1.longRanges;

            return this.DEFAULT_SLOTS_NUM == DEFAULT_SLOTS_NUM &&
                    Arrays.equals(this.rangeMap2, rangeMap2) &&
                    Objects.equals(this.hashFunction, hashFunction) &&
                    Objects.equals(this.longRanges, longRanges);
        }
        return false;
    }

    @Override
    public String getErUniqueID() {
        return  getClass().getName()+":"+DEFAULT_SLOTS_NUM + Arrays.toString(rangeMap2) + hashFunction + longRanges;
    }
}