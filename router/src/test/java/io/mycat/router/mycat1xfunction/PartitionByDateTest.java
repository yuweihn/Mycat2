package io.mycat.router.mycat1xfunction;

import io.mycat.router.ShardingTableHandler;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class PartitionByDateTest {

    @Test
    public void baseTest() {
        PartitionByDate partition = new PartitionByDate();

        Map<String, Object> prot = new HashMap<>();
        prot.put("columnValue", "id");
        prot.put("beginDate", "2014-01-01");
        prot.put("endDate", null);
        prot.put("partionDay", "10");
        prot.put("dateFormat", "yyyy-MM-dd");

        ShardingTableHandler shardingTableHandler = TableHandlerMocks.mockTableHandlerWithDataNodes(1024);
        partition.init(shardingTableHandler, prot, Collections.emptyMap());


        Assert.assertEquals(true, 0 == partition.calculateIndex("2014-01-01"));
        Assert.assertEquals(true, 0 == partition.calculateIndex("2014-01-10"));
        Assert.assertEquals(true, 1 == partition.calculateIndex("2014-01-11"));
        Assert.assertEquals(true, 12 == partition.calculateIndex("2014-05-01"));


        //////////////////////////////////////////
        prot.clear();
        //////////////////////////////////////////

        prot.put("beginDate", "2014-01-01");
        prot.put("endDate", "2014-01-31");
        prot.put("partionDay", "10");
        prot.put("dateFormat", "yyyy-MM-dd");
        partition.init(shardingTableHandler, prot, Collections.emptyMap());

//
//		/**
//		 * 0 : 01.01-01.10,02.10-02.19
//		 * 1 : 01.11-01.20,02.20-03.01
//		 * 2 : 01.21-01.30,03.02-03.12
//		 * 3  ： 01.31-02-09,03.13-03.23
//		 */
        Assert.assertEquals(true, 0 == partition.calculateIndex("2014-01-01"));
        Assert.assertEquals(true, 0 == partition.calculateIndex("2014-01-10"));
        Assert.assertEquals(true, 1 == partition.calculateIndex("2014-01-11"));
        Assert.assertEquals(true, 3 == partition.calculateIndex("2014-02-01"));
        Assert.assertEquals(true, 0 == partition.calculateIndex("2014-02-19"));
        Assert.assertEquals(true, 1 == partition.calculateIndex("2014-02-20"));
        Assert.assertEquals(true, 1 == partition.calculateIndex("2014-03-01"));
        Assert.assertEquals(true, 2 == partition.calculateIndex("2014-03-02"));
        Assert.assertEquals(true, 2 == partition.calculateIndex("2014-03-11"));
        Assert.assertEquals(true, 3 == partition.calculateIndex("2014-03-20"));


        //////////////////////////////////////////
        prot.clear();
        //////////////////////////////////////////


        prot.put("beginDate", "2014-01-01");
        prot.put("endDate", "2014-01-31");
        prot.put("partionDay", "1");
        prot.put("dateFormat", "yyyy-MM-dd");
        partition.init(shardingTableHandler, prot, Collections.emptyMap());

        //测试默认1
        Assert.assertEquals(true, 0 == partition.calculateIndex("2014-01-01"));
        Assert.assertEquals(true, 9 == partition.calculateIndex("2014-01-10"));
        Assert.assertEquals(true, 10 == partition.calculateIndex("2014-01-11"));
        Assert.assertEquals(true, 0 == partition.calculateIndex("2014-02-01"));
        System.out.println(partition.calculate("2014-02-19"));

        //测试默认1
        Assert.assertEquals(true, 0 == partition.calculateIndex("2014-01-01"));
        Assert.assertEquals(true, 9 == partition.calculateIndex("2014-01-10"));
        Assert.assertEquals(true, 10 == partition.calculateIndex("2014-01-11"));


        Assert.assertArrayEquals(new int[]{
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
                21, 22, 23, 24, 25, 26, 27, 28, 29, 30}, partition.calculateIndexRange("2014-01-01", "2014-02-10"));
        Assert.assertArrayEquals(new int[]{
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
                21, 22, 23, 24, 25, 26, 27, 28, 29, 30}, partition.calculateIndexRange("2014-01-02", null));
        Assert.assertArrayEquals(new int[]{
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, partition.calculateIndexRange(null, "2014-01-21"));
        Assert.assertArrayEquals(new int[]{
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
                21, 22, 23, 24, 25, 26, 27, 28, 29, 30}, partition.calculateIndexRange("2014-01-01", "null"));
        Assert.assertArrayEquals(new int[]{0}, partition.calculateIndexRange("null", "2014-01-01"));

        //////////////////////////////////////////
        prot.clear();
        //////////////////////////////////////////
        /**
         * "beginDate":"2021-01-01 00:00:00",
         * 				"partionDay":"1",
         * 				"endDate":"2023-12-31 23:59:59",
         * 				"dateFormat":"yyyy-MM-dd HH:mm:ss,yyyy-MM-dd,yyyy-M-d HH:mm:ss,yyyy-M-d,yyyy/MM/dd HH:mm:ss,yyyy/MM/dd",
         * 				"columnName":"time"
         */

        prot.put("beginDate", "2021-01-01 00:00:00");
        prot.put("endDate", "2023-12-31 23:59:59");
        prot.put("partionDay", "1");
        prot.put("dateFormat", "yyyy-MM-dd HH:mm:ss,yyyy-MM-dd,yyyy-M-d HH:mm:ss,yyyy-M-d,yyyy/MM/dd HH:mm:ss,yyyy/MM/dd,yyyy/MM/dd HH:mm:ss.ss.SSSSSS");
        prot.put("columnName", "time");
        partition.init(shardingTableHandler, prot, Collections.emptyMap());
        partition.calculateIndex("2021-02-23 15:25:29.0");
        partition.calculateIndex("2021-02-23 15:25:29.0");
        partition.calculateIndex("2021-02-23 15:28:19.632");
        partition.calculateIndex("2021-02-23 15:25:29.0");
    }
}