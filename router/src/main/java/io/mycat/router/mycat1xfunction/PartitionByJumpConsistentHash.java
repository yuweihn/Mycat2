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
import io.mycat.router.ShardingTableHandler;

import java.util.Map;
import java.util.Objects;

public class PartitionByJumpConsistentHash extends Mycat1xSingleValueRuleFunction {

    private static final long UNSIGNED_MASK = 0x7fffffffffffffffL;
    private static final long JUMP = 1L << 31;
    // If JDK >= 1.8, just use Long.parseUnsignedLong("2862933555777941757") instead.
    private static final long CONSTANT = Long.parseLong("286293355577794175", 10) * 10 + 7;
    private int totalBuckets;

    private static int jumpConsistentHash(final long key, final int buckets) {
        checkBuckets(buckets);
        long k = key;
        long b = -1;
        long j = 0;

        while (j < buckets) {
            b = j;
            k = k * CONSTANT + 1L;

            j = (long) ((b + 1L) * (JUMP / toDouble((k >>> 33) + 1L)));
        }
        return (int) b;
    }

    private static void checkBuckets(final int buckets) {
        if (buckets < 0) {
            throw new IllegalArgumentException("Buckets cannot be less than 0");
        }
    }

    private static double toDouble(final long n) {
        double d = n & UNSIGNED_MASK;
        if (n < 0) {
            d += 0x1.0p63;
        }
        return d;
    }

    @Override
    public String name() {
        return "PartitionByJumpConsistentHash";
    }

    @Override
    public int calculateIndex(String columnValue) {
        return jumpConsistentHash(columnValue.hashCode(), totalBuckets);
    }

    @Override
    public int[] calculateIndexRange(String beginValue, String endValue) {
        return null;
    }


    @Override
    public void init(ShardingTableHandler table, Map<String, Object> prot, Map<String, Object> ranges) {
        this.totalBuckets = Integer.parseInt(Objects.toString(prot.get("totalBuckets")));
    }

    @Override
    public boolean isSameDistribution(CustomRuleFunction customRuleFunction) {
        if (customRuleFunction == null) return false;
        if (PartitionByJumpConsistentHash.class.isAssignableFrom(customRuleFunction.getClass())) {
            PartitionByJumpConsistentHash ruleFunction = (PartitionByJumpConsistentHash) customRuleFunction;
            long totalBuckets = ruleFunction.totalBuckets;
            return this.totalBuckets == totalBuckets;
        }
        return false;
    }
    @Override
    public String getErUniqueID() {
        return  getClass().getName()+":"+ totalBuckets;
    }
}