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
import io.mycat.router.util.StringUtil;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.*;

public class PartitionByDate extends Mycat1xSingleValueRuleFunction {

    private static final long ONE_DAY = 86400000;
    private long beginDate;
    private long partionTime;
    private long endDate;
    private int nCount;
    private DateTimeFormatter[] formatter;
    private String dateFormat;

    @Override
    public String name() {
        return "PartitionByDate";
    }

    @Override
    public void init(ShardingTableHandler tableHandler, Map<String, Object> prot, Map<String, Object> ranges) {
        this.table = tableHandler;
        this.properties = prot;
        this.ranges = ranges;

        String startBeginDate = Objects.toString(prot.get("beginDate"));
        String startEndDate = (String) (prot.get("endDate"));
        String startPartionDay = Objects.toString(prot.get("partionDay"));
        dateFormat = Objects.toString(prot.get("dateFormat"));
        String[] split = dateFormat.split(",");
        formatter = new DateTimeFormatter[split.length];
        for (int i = 0; i < split.length; i++) {
            formatter[i] = DateTimeFormatter.ofPattern(split[i]);
        }
        beginDate = getTime(startBeginDate);
        endDate = 0L;
        nCount = 0;
        partionTime = Long.parseLong(startPartionDay) * ONE_DAY;
        if (!StringUtil.isEmpty(startEndDate)) {
            endDate = getTime(startEndDate);
            nCount = (int) ((endDate - beginDate) / partionTime) + 1;
        }

    }

    private long getTime(String startBeginDate) {
        if(startBeginDate == null || "null".equalsIgnoreCase(startBeginDate)){
            return -1L;
        }
        for (DateTimeFormatter dateTimeFormatter : formatter) {
            try {
                return dateTimeFormatter.parse(startBeginDate).getLong(ChronoField.DAY_OF_YEAR) * ONE_DAY;
            } catch (DateTimeParseException e) {
                //skip
            }
        }
        try {
            return Timestamp.valueOf(startBeginDate).toLocalDateTime().getLong(ChronoField.DAY_OF_YEAR) * ONE_DAY;
        } catch (DateTimeParseException e) {
            //skip
        }
        throw new IllegalArgumentException(
                "columnValue:" + startBeginDate + " Please check if the format satisfied."+dateFormat);
    }

    @Override
    public int calculateIndex(String columnValue) {
        long targetTime = getTime(columnValue);
        return innerCalculate(targetTime);
    }

    private int innerCalculate(long targetTime) {
        int targetPartition = (int) ((targetTime - beginDate) / partionTime);
        if (targetTime > endDate && nCount != 0) {
            targetPartition = targetPartition % nCount;
        }
        return targetPartition;
    }

    @Override
    public int[] calculateIndexRange(String beginValue, String endValue) {
        long beginDate = getTime(beginValue);
        long endDate = getTime(endValue);
        if(endDate == -1){
            endDate = this.endDate;
        }
        if(beginDate == -1){
            beginDate = this.beginDate;
        }
        ArrayList<Integer> list = new ArrayList<>();
        while (beginDate <= endDate) {
            int nodeValue = innerCalculate(beginDate);
            if (Collections.frequency(list, nodeValue) < 1) {
                list.add(nodeValue);
            }
            beginDate += ONE_DAY;
        }
        if(list.isEmpty()){
            return null;
        }
        return ints(list);
    }

    @Override
    public boolean isSameDistribution(CustomRuleFunction customRuleFunction) {
        if (customRuleFunction == null) return false;
        if (PartitionByDate.class.isAssignableFrom(customRuleFunction.getClass())) {
            PartitionByDate partitionByDate = (PartitionByDate) customRuleFunction;
            long beginDate = partitionByDate.beginDate;
            long partionTime = partitionByDate.partionTime;
            long endDate = partitionByDate.endDate;
            int nCount = partitionByDate.nCount;
            String dateFormat = partitionByDate.dateFormat;

            return this.beginDate == beginDate &&
                    this.partionTime == partionTime &&
                    this.endDate == endDate &&
                    this.nCount == nCount &&
                    Objects.equals(this.dateFormat, dateFormat);
        }
        return false;
    }

    @Override
    public String getErUniqueID() {
        return  getClass().getName()+":"+ beginDate + partionTime + endDate + nCount + dateFormat;
    }

}