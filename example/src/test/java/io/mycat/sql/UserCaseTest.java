package io.mycat.sql;

import com.alibaba.druid.util.JdbcUtils;
import io.mycat.assemble.MycatTest;
import io.mycat.config.*;
import io.mycat.hbt.SchemaConvertor;
import io.mycat.hint.*;
import io.mycat.router.function.IndexDataNode;
import io.mycat.router.mycat1xfunction.PartitionByFileMap;
import io.mycat.router.mycat1xfunction.PartitionByHotDate;
import io.mycat.util.ByteUtil;
import io.mycat.util.StringUtil;
import io.vertx.core.json.Json;
import org.apache.groovy.util.Maps;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.boot.autoconfigure.quartz.QuartzProperties;

import javax.annotation.concurrent.NotThreadSafe;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.Date;
import java.util.concurrent.TimeUnit;


@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class UserCaseTest implements MycatTest {

    @Test
    public void case1() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT)) {
            execute(mycatConnection, RESET_CONFIG);

            execute(mycatConnection, "DROP DATABASE db1");


            execute(mycatConnection, "CREATE DATABASE db1");


            execute(mycatConnection, CreateDataSourceHint
                    .create("ds0",
                            DB1));

            execute(mycatConnection,
                    CreateClusterHint.create("c0",
                            Arrays.asList("ds0"), Collections.emptyList()));

            execute(mycatConnection, "USE `db1`;");

            execute(mycatConnection, "CREATE TABLE `travelrecord2` (\n" +
                    "  `id` bigint(20) NOT NULL KEY,\n" +
                    "  `user_id` varchar(100) CHARACTER SET utf8 DEFAULT NULL,\n" +
                    "  `traveldate` datetime(6) DEFAULT NULL,\n" +
                    "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                    "  `days` int(11) DEFAULT NULL,\n" +
                    "  `blob` longblob DEFAULT NULL\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                    "tbpartition by YYYYMM(traveldate) tbpartitions 12;");

            execute(mycatConnection, "CREATE TABLE `user` (\n" +
                    "  `id` int NOT NULL,\n" +
                    "  `name` varchar(45) DEFAULT NULL,\n" +
                    "  PRIMARY KEY (`id`)\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8;");

            deleteData(mycatConnection, "db1", "travelrecord2");

            executeQuery(mycatConnection, "SELECT t1.id,t1.name,t2.count FROM db1.user as t1\n" +
                    "left join (select count(1) as `count`,`user_id` from travelrecord2 group by `user_id`) \n" +
                    "as t2 on `t1`.`id` = `t2`.`user_id`;");

            executeQuery(mycatConnection, "SELECT t1.id,t1.name,t2.count FROM db1.user as t1\n" +
                    "left join (select count(1) as `count`,`user_id` from travelrecord2 group by `user_id`) \n" +
                    "as `t2` on `t1`.`id` = `t2`.`user_id`;");
            execute(mycatConnection, "use db1");

            execute(mycatConnection, "START TRANSACTION;\n" +
                    "INSERT INTO `travelrecord2` (id,`blob`, `days`, `fee`, `traveldate`, `user_id`)\n" +
                    "VALUES (1,NULL, 3, 3, timestamp('2021-02-21 12:23:42.058156'), 'tom');\n" +
                    "SELECT ROW_COUNT();\n" +
                    "COMMIT;");
            execute(mycatConnection, "" +
                    "UPDATE  `travelrecord2` SET id = 1 where id = 1;" +
                    "SELECT ROW_COUNT();\n");
            execute(mycatConnection, "" +
                    "UPDATE  `user` SET id = 1 where id = 1;SELECT * from `user`; " +
                    "SELECT ROW_COUNT();\n");
            executeQuery(mycatConnection, "SELECT ROW_COUNT();");
            deleteData(mycatConnection, "db1", "travelrecord2");
            execute(mycatConnection, "INSERT INTO `travelrecord2`(`id`,`user_id`,`traveldate`,`fee`,`days`,`blob`)\n" +
                    "VALUES (1,2,timestamp('2021-02-22 18:34:05.983692'),3.5,4,NULL)");

            Statement statement = mycatConnection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT traveldate FROM travelrecord2 WHERE id = 1;");
            resultSet.next();
            Timestamp traveldate = resultSet.getTimestamp("traveldate");
//            Assert.assertEquals(Timestamp.valueOf("2021-02-23 10:34:05.983692"), traveldate);
            resultSet.close();
//            = executeQuery(mycatConnection, );
//            Assert.assertTrue(maps.get(0).get().toString().endsWith("983692"));//!= 05.983692000
            List<Map<String, Object>> maps;
            maps = executeQuery(mycatConnection, "SELECT * FROM travelrecord2 WHERE traveldate = '2021-02-22 18:34:05.983692';");
//            Assert.assertTrue(!maps.isEmpty());
            maps = executeQuery(mycatConnection, "SELECT * FROM travelrecord2 WHERE traveldate = timestamp('2021-02-22 18:34:05.983692');");
//            Assert.assertTrue(!maps.isEmpty());
            maps = executeQuery(mycatConnection, "SELECT * FROM travelrecord2 WHERE CONVERT(traveldate,date) = '2021-2-22';");
            execute(mycatConnection, "START TRANSACTION\n" +
                    "INSERT INTO `travelrecord2`(`id`,`user_id`,`traveldate`,`fee`,`days`,`blob`)\n" +
                    "VALUES \n" +
                    "(6,2,timestamp('2021-02-22 18:34:05.983692'),4.5,4,NULL),\n" +
                    "(7,2,timestamp('2021-02-22 18:34:05.983692'),4.5,4,NULL),\n" +
                    "(8,2,timestamp('2021-02-22 18:34:05.983692'),4.5,4,NULL);\n" +
                    "COMMIT;");
            deleteData(mycatConnection, "db1", "travelrecord2");
            execute(mycatConnection, "START TRANSACTION\n" +
                    "INSERT INTO `travelrecord2`(`id`,`user_id`,`traveldate`,`fee`,`days`,`blob`)\n" +
                    "VALUES \n" +
                    "(6,2,'2021-02-22 18:34:05.983692',4.5,4,NULL),\n" +
                    "(7,2,'2021-02-22 18:34:05.983692',4.5,4,NULL),\n" +
                    "(8,2,'2021-02-22 18:34:05.983692',4.5,4,NULL);\n" +
                    "COMMIT;");
        }
    }

    @Test
    public void case2() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);
             Connection mysqlConnection = getMySQLConnection(DB1)) {
            execute(mycatConnection, RESET_CONFIG);

            execute(mycatConnection, "DROP DATABASE db1");


            execute(mycatConnection, "CREATE DATABASE db1");


            execute(mycatConnection, CreateDataSourceHint
                    .create("ds0",
                            DB1));

            execute(mycatConnection,
                    CreateClusterHint.create("c0",
                            Arrays.asList("ds0"), Collections.emptyList()));

            execute(mycatConnection, "USE `db1`;");

            execute(mycatConnection, "CREATE TABLE `user` (\n" +
                    "  `id` int NOT NULL,\n" +
                    "  `name` varchar(45) DEFAULT NULL,\n" +
                    "\t`is_enable` tinyint(1) not null default 1,\n" +
                    "  PRIMARY KEY (`id`)\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8;");
            deleteData(mycatConnection, "db1", "user");
            execute(mycatConnection, "insert into `user`(`id`,`name`,`is_enable`) values (1,'abc',1);");
            List<Map<String, Object>> maps = executeQuery(mycatConnection, "SELECT * FROM `user`;");//[{id=1, name=abc, is_enable=1}]
            List<Map<String, Object>> right = executeQuery(mysqlConnection, "SELECT * FROM db1.`user`;");//[{id=1, name=abc, is_enable=true}]
            Assert.assertTrue("[{id=1, name=abc, is_enable=1}]".equals(maps.toString()) || "[{id=1, name=abc, is_enable=true}]".equals(maps.toString()));
//            Assert.assertArrayEquals(new byte[]{1}, ((byte[]) maps.get(0).get("is_enable")));
        }
    }


    @Test
    public void case3() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);
             Connection mysqlConnection = getMySQLConnection(DB1)) {
            execute(mycatConnection, RESET_CONFIG);

            execute(mycatConnection, "DROP DATABASE db1");


            execute(mycatConnection, "CREATE DATABASE db1");


            execute(mycatConnection, CreateDataSourceHint
                    .create("ds0",
                            DB1));

            execute(mycatConnection,
                    CreateClusterHint.create("c0",
                            Arrays.asList("ds0"), Collections.emptyList()));

            execute(mycatConnection, "USE `db1`;");

            execute(mycatConnection, "CREATE TABLE `user` (\n" +
                    "  `id` int NOT NULL,\n" +
                    "  `name` varchar(45) DEFAULT NULL,\n" +
                    "\t`is_enable` bit(1) not null default 1,\n" +
                    "  PRIMARY KEY (`id`)\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8;");
            deleteData(mycatConnection, "db1", "user");
            execute(mycatConnection, "insert into `user`(`id`,`name`,`is_enable`) values (1,'abc',1);");
            List<Map<String, Object>> maps = executeQuery(mycatConnection, "SELECT * FROM `user`;");//[{id=1, name=abc, is_enable=1}]
            List<Map<String, Object>> right = executeQuery(mysqlConnection, "SELECT * FROM db1.`user`;");//[{id=1, name=abc, is_enable=true}]
            Assert.assertTrue("[{id=1, name=abc, is_enable=1}]".equals(maps.toString()) || "[{id=1, name=abc, is_enable=true}]".equals(maps.toString()));
        }
    }

    @Test
    public void case4() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT)) {
            execute(mycatConnection, RESET_CONFIG);

            execute(mycatConnection, "DROP DATABASE cloud");


            execute(mycatConnection, "CREATE DATABASE cloud");


            execute(mycatConnection, CreateDataSourceHint
                    .create("ds0",
                            DB1));

            execute(mycatConnection, CreateDataSourceHint
                    .create("ds1",
                            DB2));

            execute(mycatConnection,
                    CreateClusterHint.create("c0",
                            Arrays.asList("ds0"), Collections.emptyList()));

            execute(mycatConnection,
                    CreateClusterHint.create("c1",
                            Arrays.asList("ds1"), Collections.emptyList()));

            execute(mycatConnection, "USE `cloud`;");

            execute(mycatConnection, "CREATE TABLE IF NOT EXISTS `service` (\n" +
                    "  `id` bigint(20) NOT NULL,\n" +
                    "  `name` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,\n" +
                    "  PRIMARY KEY (`id`)\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8;");

            execute(mycatConnection, "CREATE TABLE IF NOT EXISTS `user` (\n" +
                    "  `id` bigint(20) NOT NULL,\n" +
                    "  `name` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,\n" +
                    "  PRIMARY KEY (`id`)\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8;");

            execute(mycatConnection, "CREATE TABLE cloud.log (\n" +
                    "  `id` BIGINT(20) DEFAULT NULL,\n" +
                    "  `user_id` BIGINT(20) DEFAULT NULL,\n" +
                    "  `service_id` INT(11) DEFAULT NULL,\n" +
                    "  `submit_time` DATETIME DEFAULT NULL\n" +
                    ") ENGINE=INNODB DEFAULT CHARSET=utf8  dbpartition BY YYYYDD(submit_time) dbpartitions 2 tbpartition BY MOD_HASH (id) tbpartitions 8;\n");

            deleteData(mycatConnection, "cloud", "service");
            deleteData(mycatConnection, "cloud", "user");
            deleteData(mycatConnection, "cloud", "log");

            String sql1 = "SELECT log.id AS log_id,user.name AS user_name, service.name AS service_name,log.submit_time\n" +
                    "FROM\n" +
                    "`cloud`.`log` INNER JOIN `cloud`.`user`\n" +
                    "ON log.user_id = user.id\n" +
                    "INNER JOIN `cloud`.`service`\n" +
                    "ON service.id  = service_id\n" +
                    "ORDER BY log.submit_time DESC LIMIT 0,20;";
            System.out.println(sql1);
            String explain1 = explain(mycatConnection, sql1);
            System.out.println(explain1);
            executeQuery(mycatConnection, sql1);

            Assert.assertTrue(explain1.contains("MycatView(distribution=[[cloud.log]]"));

            // String sql2 = "/*+MYCAT:use_values_join(log,user) use_values_join(log,service) */ SELECT log.id AS log_id,user.name AS user_name, service.name AS service_name,log.submit_time FROM (SELECT log.`id` ,log.`service_id`,log.`submit_time`,log.`user_id` FROM `cloud`.`log`  WHERE log.submit_time = '2021-5-31' ORDER BY log.submit_time DESC LIMIT 0,20) AS `log` INNER JOIN `cloud`.`user` ON log.user_id = user.id INNER JOIN `cloud`.`service`  ON service.id  = log.service_id ORDER BY log.submit_time DESC LIMIT 0,20;";
            String sql2 = " SELECT log.id AS log_id,user.name AS user_name, service.name AS service_name,log.submit_time FROM (SELECT log.`id` ,log.`service_id`,log.`submit_time`,log.`user_id` FROM `cloud`.`log`  WHERE log.submit_time = '2021-5-31' ORDER BY log.submit_time DESC LIMIT 0,20) AS `log` INNER JOIN `cloud`.`user` ON log.user_id = user.id INNER JOIN `cloud`.`service`  ON service.id  = log.service_id ORDER BY log.submit_time DESC LIMIT 0,20;";

            System.out.println(sql2);
            String explain2 = explain(mycatConnection, sql2);
            System.out.println(explain2);
            Assert.assertEquals(true, explain2.contains("TableLook") || explain2.contains("SortMergeJoin"));
            executeQuery(mycatConnection, sql2);

            //test transaction
            mycatConnection.setAutoCommit(false);
            executeQuery(mycatConnection, sql2);
            mycatConnection.setAutoCommit(true);
        }
    }

    @Test
    public void test548() throws Exception {
        try (Connection mycat = getMySQLConnection(DB_MYCAT);) {
            execute(mycat, RESET_CONFIG);
            String db = "db1";
            String tableName = "sharding";
            execute(mycat, "drop database if EXISTS " + db);
            execute(mycat, "create database " + db);
            execute(mycat, "use " + db);


            execute(mycat, CreateClusterHint
                    .create("c0", Arrays.asList("prototypeDs"), Arrays.asList()));

            execute(
                    mycat,
                    CreateTableHint
                            .createSharding(db, tableName,
                                    "CREATE TABLE db1.`sharding` (\n" +
                                            "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                                            "  `user_id` varchar(100) DEFAULT NULL,\n" +
                                            "  `create_time` date DEFAULT NULL,\n" +
                                            "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                                            "  `days` int DEFAULT NULL,\n" +
                                            "  `blob` longblob,\n" +
                                            "  PRIMARY KEY (`id`),\n" +
                                            "  KEY `id` (`id`)\n" +
                                            ") ENGINE=InnoDB  DEFAULT CHARSET=utf8",
                                    ShardingBackEndTableInfoConfig.builder()
                                            .schemaNames(db)
                                            .tableNames("sharding_0,sharding_1")
                                            .targetNames("c0").build(),
                                    ShardingFunction.builder()
                                            .clazz(PartitionByHotDate.class.getCanonicalName())
                                            .properties(Maps.of(
                                                    "dateFormat", "yyyy-MM-dd",
                                                    "lastTime", 90,
                                                    "partionDay", 180,
                                                    "columnName", "create_time"
                                            )).build())
            );
            deleteData(mycat, db, tableName);
            execute(mycat, "insert into " + tableName + " (create_time) VALUES ('2021-06-30')");
            execute(mycat, "insert into " + tableName + "(create_time) VALUES ('2021-06-29')");
            execute(mycat, "insert into " + tableName + " (create_time) VALUES ('2021-06-29')");
            List<Map<String, Object>> maps = executeQuery(mycat, "select * from db1.sharding");
            Assert.assertEquals(3, maps.size());
            List<Map<String, Object>> maps1 = executeQuery(mycat, "select * from db1.sharding where create_time = '2021-06-30'");
            Assert.assertEquals(1, maps1.size());
            System.out.println();

//            execute(mycat, "drop database " + db);
        }
    }

    //测试 `1cloud` 标识符
    //测试 预处理 id = 1
    //测试bit tiny类型
    @Test
    public void case5() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT_PSTMT)) {
            execute(mycatConnection, RESET_CONFIG);

            execute(mycatConnection, "DROP DATABASE `1cloud`");


            execute(mycatConnection, "CREATE DATABASE `1cloud`");


            execute(mycatConnection, CreateDataSourceHint
                    .create("ds0",
                            DB1));

            execute(mycatConnection, CreateDataSourceHint
                    .create("ds1",
                            DB2));

            execute(mycatConnection,
                    CreateClusterHint.create("c0",
                            Arrays.asList("ds0"), Collections.emptyList()));

            execute(mycatConnection, "USE `1cloud`;");

            execute(mycatConnection, "CREATE TABLE IF NOT EXISTS `1service` (\n" +
                    "  `b` bit(1) NOT NULL,\n" +
                    "  `tiny` TINYINT(4)," +
                    " `s` varchar(20) NOT NULL,\n" +
                    "  PRIMARY KEY (`tiny`)\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8;");

            execute(mycatConnection, "CREATE TABLE `1cloud`.`1log` (\n" +
                    "  `id` BIGINT(20) DEFAULT NULL,\n" +
                    "  `user_id` BIGINT(20) DEFAULT NULL,\n" +
                    "  `service_id` INT(11) DEFAULT NULL,\n" +
                    "  `submit_time` DATETIME DEFAULT NULL\n" +
                    ") ENGINE=INNODB DEFAULT CHARSET=utf8  dbpartition BY YYYYDD(submit_time) dbpartitions 1 tbpartition BY MOD_HASH (id) tbpartitions 1;\n");
            deleteData(mycatConnection, "`1cloud`", "`1service`");
            deleteData(mycatConnection, "`1cloud`", "`1log`");
            count(mycatConnection, "`1cloud`", "`1service`");
            count(mycatConnection, "`1cloud`", "`1log`");
            execute(mycatConnection, "insert `1cloud`.`1log` (id) values (1)");
            execute(mycatConnection, "insert `1cloud`.`1service`  values (1,1,'2')");
            List<Map<String, Object>> maps = JdbcUtils.executeQuery(mycatConnection, "select * from `1cloud`.`1log` where id = ?", Arrays.asList(1L));
            Assert.assertEquals(1, maps.size());
            List<Map<String, Object>> maps2 = JdbcUtils.executeQuery(mycatConnection, "select * from `1cloud`.`1service`", Collections.emptyList());
            Assert.assertEquals(3, maps2.get(0).size());
            Assert.assertEquals("[{b=true, tiny=1, s=2}]", maps2.toString());
            System.out.println();
        }
    }

    @Test
    public void case6() throws Exception {
        String table = "sharding";
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT_PSTMT)) {
            execute(mycatConnection, RESET_CONFIG);

            execute(mycatConnection, "DROP DATABASE `1cloud`");


            execute(mycatConnection, "CREATE DATABASE `1cloud`");


            execute(mycatConnection, "USE `1cloud`;");

            execute(
                    mycatConnection,
                    CreateTableHint
                            .createSharding("1cloud", table,
                                    "create table " + table + "(\n" +
                                            "id int(11) NOT NULL AUTO_INCREMENT,\n" +
                                            "user_id int(11) ,\n" +
                                            "user_name varchar(128), \n" +
                                            "PRIMARY KEY (`id`) \n" +
                                            ")ENGINE=InnoDB DEFAULT CHARSET=utf8 ",
                                    ShardingBackEndTableInfoConfig.builder()
                                            .schemaNames("c")
                                            .tableNames("file_$0-2")
                                            .targetNames("prototype").build(),
                                    ShardingFunction.builder()
                                            .clazz(PartitionByFileMap.class.getCanonicalName())
                                            .properties(Maps.of(
                                                    "defaultNode", "0",
                                                    "type", "Integer",
                                                    "columnName", "id"
                                            )).ranges(Maps.of(
                                                    "130100", "0",
                                                    "130200", "1",
                                                    "130300", "2"
                                            )).build())
            );
            deleteData(mycatConnection, "`1cloud`", table);
            Assert.assertEquals(0, count(mycatConnection, "`1cloud`", table));
            execute(mycatConnection, "insert `1cloud`." +
                    table +
                    " (id) values (130100)");
            Assert.assertEquals(1, count(mycatConnection, "`1cloud`", table));
            String zero_w = explain(mycatConnection, "insert `1cloud`." +
                    table +
                    " (id) values (130100)");
            String one_w = explain(mycatConnection, "insert `1cloud`." +
                    table +
                    " (id) values (130200)");
            String second_r = explain(mycatConnection, "insert `1cloud`." +
                    table +
                    " (id) values (130300)");

            Assert.assertTrue(zero_w.contains("file_0"));
            Assert.assertTrue(one_w.contains("file_1"));
            Assert.assertTrue(second_r.contains("file_2"));

            String zero_r = explain(mycatConnection, "select * from " + table + " where id = " + 130100);
            String one_r = explain(mycatConnection, "select * from " + table + " where id = " + 130200);
            String two_r = explain(mycatConnection, "select * from " + table + " where id = " + 130300);

            Assert.assertTrue(zero_r.contains("file_0"));
            Assert.assertTrue(one_r.contains("file_1"));
            Assert.assertTrue(two_r.contains("file_2"));

            System.out.println();
        }
    }

    @Test
    public void case7() throws Exception {
        String table = "sharding";
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT_PSTMT)) {
            execute(mycatConnection, RESET_CONFIG);
            execute(mycatConnection,
                    CreateClusterHint.create("c0",
                            Arrays.asList("prototypeDs"), Collections.emptyList()));
            execute(mycatConnection, "DROP DATABASE `1cloud`");


            execute(mycatConnection, "CREATE DATABASE `1cloud`");


            execute(mycatConnection, "USE `1cloud`;");

            execute(mycatConnection, "CREATE TABLE `1cloud`.`1log` (\n" +
                    "  `id` BIGINT(20) DEFAULT NULL,\n" +
                    "  `user_id` BIGINT(20) DEFAULT NULL,\n" +
                    "  `service_id` INT(11) DEFAULT NULL,\n" +
                    "  `submit_time` DATETIME DEFAULT NULL\n" +
                    ") ENGINE=INNODB DEFAULT CHARSET=utf8  dbpartition BY YYYYDD(submit_time) dbpartitions 1 tbpartition BY MOD_HASH (id) tbpartitions 1;\n");


            String sql = "select any_value(submit_time) from `1log` where submit_time between '2019-5-31' and '2019-6-21' group by DATE_FORMAT(submit_time,'%Y-%m')";
            String explain = explain(mycatConnection, sql);
            executeQuery(mycatConnection, sql);
            System.out.println();
            execute(mycatConnection, "USE `1cloud`;");
            execute(
                    mycatConnection,
                    CreateTableHint
                            .createSharding("1cloud", "stat_ad_sdk",
                                    "CREATE TABLE stat_ad_sdk (\n" +
                                            "ad_id int unsigned DEFAULT NULL COMMENT '广告位id',\n" +
                                            "ad_uuid varchar(50) DEFAULT NULL COMMENT '渠道广告位id',\n" +
                                            "ad_name varchar(500) DEFAULT NULL COMMENT '广告位名称',\n" +
                                            "ad_type varchar(50) DEFAULT NULL COMMENT '广告位类型',\n" +
                                            "uid int unsigned DEFAULT NULL COMMENT '开发者id',\n" +
                                            "user_name varchar(500) DEFAULT NULL COMMENT '开发者名',\n" +
                                            "app_id int unsigned DEFAULT NULL COMMENT '应用id',\n" +
                                            "app_name varchar(500) DEFAULT NULL COMMENT '应用名',\n" +
                                            "channel_id int unsigned DEFAULT NULL COMMENT '渠道id',\n" +
                                            "date date DEFAULT NULL COMMENT '日期',\n" +
                                            "req int unsigned NOT NULL DEFAULT '0' COMMENT '请求量',\n" +
                                            "fill_rate varchar(20) DEFAULT NULL COMMENT '填充率',\n" +
                                            "`show` int unsigned NOT NULL DEFAULT '0' COMMENT '展示量',\n" +
                                            "click int unsigned NOT NULL DEFAULT '0' COMMENT '点击量',\n" +
                                            "video_error int unsigned NOT NULL DEFAULT '0' COMMENT '视频播放错误量',\n" +
                                            "video_not_complete int unsigned NOT NULL DEFAULT '0' COMMENT '视频未完整播放量',\n" +
                                            "version varchar(50) DEFAULT NULL COMMENT 'sdk版本',\n" +
                                            "UNIQUE KEY dateAdIdVer (date,ad_uuid,version) USING BTREE\n" +
                                            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COMMENT='广告sdk日统计数据';",
                                    ShardingBackEndTableInfoConfig.builder()
                                            .schemaNames("c")
                                            .tableNames("stat_ad_sdk_$0-11")
                                            .targetNames("prototype").build(),
                                    ShardingFunction.builder()
                                            .clazz(io.mycat.router.mycat1xfunction.PartitionByMonth.class.getCanonicalName())
                                            .properties(Maps.of(
                                                    "beginDate", "2019-01-01",
                                                    "endDate", "2099-12-01",
                                                    "dateFormat", "yyyy-MM-dd",
                                                    "columnName", "date"
                                            )).build())
            );

            sql = "select any_value(date) from `stat_ad_sdk` where date between '2019-5-01' and '2019-05-31' group by DATE_FORMAT(date,'%Y-%m')";
            explain = explain(mycatConnection, sql);
            executeQuery(mycatConnection, sql);
            System.out.println();

        }
    }

    @Test
    public void case8() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);
             Connection mysqlConnection = getMySQLConnection(DB1)) {
            execute(mycatConnection, RESET_CONFIG);

            execute(mycatConnection, "DROP DATABASE db1");


            execute(mycatConnection, "CREATE DATABASE db1");
            execute(mycatConnection, "use db1");
            execute(mycatConnection, "CREATE TABLE `sys_menu` (\n" +
                    "  `menu_id` bigint(20) NOT NULL AUTO_INCREMENT,\n" +
                    "  `menu_name` varchar(50) NOT NULL ,\n" +
                    "  `parent_id` bigint(20) DEFAULT '0' ,\n" +
                    "  `order_num` int(4) DEFAULT '0',\n" +
                    "  `path` varchar(200) DEFAULT '' ,\n" +
                    "  `component` varchar(255) DEFAULT NULL ,\n" +
                    "  `is_frame` int(1) DEFAULT '1' ,\n" +
                    "  `is_cache` int(1) DEFAULT '0',\n" +
                    "  `menu_type` varchar(1) DEFAULT '' ,\n" +
                    "  `visible` varchar(1) DEFAULT '0',\n" +
                    "  `status` varchar(1) DEFAULT '0' ,\n" +
                    "  `perms` varchar(100) DEFAULT NULL ,\n" +
                    "  `icon` varchar(100) DEFAULT '#' ,\n" +
                    "  `create_by` varchar(64) DEFAULT '' ,\n" +
                    "  `create_time` datetime DEFAULT NULL ,\n" +
                    "  `update_by` varchar(64) DEFAULT '',\n" +
                    "  `update_time` datetime DEFAULT NULL,\n" +
                    "  `remark` varchar(500) DEFAULT '',\n" +
                    "  PRIMARY KEY (`menu_id`)\n" +
                    ") ENGINE=InnoDB AUTO_INCREMENT=1080 DEFAULT CHARSET=utf8 ;");
            deleteData(mycatConnection, "db1", "sys_menu");
            execute(mycatConnection, "INSERT INTO `sys_menu` VALUES ('1', '系统管理', '0', '6', 'common', null, '1', '0', 'M', '0', '0', '', 'build', 'admin', '2021-04-15 12:06:30', 'admin', null, '系统管理目录');");
            String sql = "select * from db1.sys_menu";

            Statement mycatStatement = mycatConnection.createStatement();
            ResultSet mycatresultSet = mycatStatement.executeQuery(sql);
            ResultSetMetaData mycatmetaData = mycatresultSet.getMetaData();

            Statement mysqlstatement = mysqlConnection.createStatement();
            ResultSet mysqlresultSet = mysqlstatement.executeQuery(sql);
            ResultSetMetaData mysqlmetaData = mysqlresultSet.getMetaData();

            Assert.assertEquals(mysqlmetaData.getColumnCount(), mycatmetaData.getColumnCount());
            for (int i = 1; i <= mysqlmetaData.getColumnCount(); i++) {
                int mysqlcolumnType = mysqlmetaData.getColumnType(i);
                int mysqlNullable = mysqlmetaData.isNullable(i);
                int mycatcolumnType = mycatmetaData.getColumnType(i);
                int mycatNullable = mycatmetaData.isNullable(i);
                Assert.assertEquals(mysqlcolumnType, mycatcolumnType);
                Assert.assertEquals(mysqlNullable, mycatNullable);
            }
            System.out.println();
        }
    }

    @Test
    public void case9() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT)) {


            execute(mycatConnection, RESET_CONFIG);

            execute(mycatConnection, "DROP DATABASE db1");


            execute(mycatConnection, "CREATE DATABASE db1");

            execute(mycatConnection, CreateDataSourceHint
                    .create("ds0",
                            DB1));

            execute(mycatConnection,
                    CreateClusterHint.create("c0",
                            Arrays.asList("ds0"), Collections.emptyList()));


            testBlob(mycatConnection, "CREATE TABLE db1.`travelrecord` (\n" +
                    "  `id` bigint(20) NOT NULL KEY,\n" +
                    "  `user_id` varchar(100) CHARACTER SET utf8 DEFAULT NULL,\n" +
                    "  `traveldate` datetime(6) DEFAULT NULL,\n" +
                    "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                    "  `days` int(11) DEFAULT NULL,\n" +
                    "  `blob` longblob DEFAULT NULL\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                    "tbpartition by mod_hash(id) tbpartitions 1;");
            testBlob(mycatConnection, "CREATE TABLE db1.`travelrecord` (\n" +
                    "  `id` bigint(20) NOT NULL KEY,\n" +
                    "  `user_id` varchar(100) CHARACTER SET utf8 DEFAULT NULL,\n" +
                    "  `traveldate` datetime(6) DEFAULT NULL,\n" +
                    "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                    "  `days` int(11) DEFAULT NULL,\n" +
                    "  `blob` longblob DEFAULT NULL\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                    "");
        }
    }

    @Test
    public void case10() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT_PSTMT)) {


            execute(mycatConnection, RESET_CONFIG);

            execute(mycatConnection, "DROP DATABASE db1");


            execute(mycatConnection, "CREATE DATABASE db1");

            execute(mycatConnection, CreateDataSourceHint
                    .create("ds0",
                            DB1));

            execute(mycatConnection,
                    CreateClusterHint.create("c0",
                            Arrays.asList("ds0"), Collections.emptyList()));


            testBlob(mycatConnection, "CREATE TABLE db1.`travelrecord` (\n" +
                    "  `id` bigint(20) NOT NULL KEY,\n" +
                    "  `user_id` varchar(100) CHARACTER SET utf8 DEFAULT NULL,\n" +
                    "  `traveldate` datetime(6) DEFAULT NULL,\n" +
                    "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                    "  `days` int(11) DEFAULT NULL,\n" +
                    "  `blob` longblob DEFAULT NULL\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                    "tbpartition by mod_hash(id) tbpartitions 1;");
            testBlob(mycatConnection, "CREATE TABLE db1.`travelrecord` (\n" +
                    "  `id` bigint(20) NOT NULL KEY,\n" +
                    "  `user_id` varchar(100) CHARACTER SET utf8 DEFAULT NULL,\n" +
                    "  `traveldate` datetime(6) DEFAULT NULL,\n" +
                    "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                    "  `days` int(11) DEFAULT NULL,\n" +
                    "  `blob` longblob DEFAULT NULL\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                    "");
        }
    }

    @Test
    public void case11() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT_PSTMT)) {


            execute(mycatConnection, RESET_CONFIG);

            execute(mycatConnection, "DROP DATABASE db1");


            execute(mycatConnection, "CREATE DATABASE db1");

            execute(mycatConnection, CreateDataSourceHint
                    .create("ds0",
                            DB1));

            execute(mycatConnection,
                    CreateClusterHint.create("c0",
                            Arrays.asList("ds0"), Collections.emptyList()));


            testBlob(mycatConnection, "CREATE TABLE db1.`travelrecord` (\n" +
                    "  `id` bigint(20) NOT NULL KEY,\n" +
                    "  `user_id` varchar(100) CHARACTER SET utf8 DEFAULT NULL,\n" +
                    "  `traveldate` datetime(6) DEFAULT NULL,\n" +
                    "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                    "  `days` int(11) DEFAULT NULL,\n" +
                    "  `blob` longblob DEFAULT NULL\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                    "tbpartition by mod_hash(id) tbpartitions 1;");
            String explain = explain(mycatConnection, "select count(*) from db1.travelrecord");
            Assert.assertTrue(explain.contains("MycatHashAggregate(group=[{}], count(*)=[$SUM0($0)])\n" +
                    "  MycatView(distribution=[[db1.travelrecord]])\n" +
                    "Each(targetName=c0, sql=SELECT COUNT(*) AS `count(*)` FROM db1_0.travelrecord_0 AS `travelrecord`)"));
            System.out.println();
        }
    }

    /**
     * 测试数据源更新
     *
     * @throws Exception
     */
    @Test
    public void case12() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);
             Connection readMysql = getMySQLConnection(DB2);
        ) {
            execute(mycatConnection, RESET_CONFIG);

            DatasourceConfig ds0 = new DatasourceConfig();
            ds0.setName("ds0");
            ds0.setUrl(DB1);
            ds0.setPassword(CreateDataSourceHint.PASSWORD);
            ds0.setUser(CreateDataSourceHint.USER_NAME);
            ds0.setInstanceType("READ_WRITE");

            DatasourceConfig ds1 = new DatasourceConfig();
            ds1.setName("ds1");
            ds1.setUrl(DB2);
            ds1.setPassword(CreateDataSourceHint.PASSWORD);
            ds1.setUser(CreateDataSourceHint.USER_NAME);
            ds1.setInstanceType("READ_WRITE");

            execute(mycatConnection, CreateDataSourceHint
                    .create(ds0));

            execute(mycatConnection, CreateDataSourceHint
                    .create(ds1));

            execute(mycatConnection,
                    CreateClusterHint.create("prototype",
                            Arrays.asList("ds0"), Arrays.asList("ds1")));

            execute(mycatConnection, "CREATE DATABASE IF NOT EXISTS db1");
            execute(mycatConnection, "CREATE TABLE IF NOT EXISTS db1.`tbl`(\n" +
                    "   `id` INT UNSIGNED AUTO_INCREMENT," +
                    "   PRIMARY KEY ( `id` )\n" +
                    ")ENGINE=InnoDB DEFAULT CHARSET=utf8;");


            execute(readMysql, "CREATE DATABASE IF NOT EXISTS db1");
            execute(readMysql, "CREATE TABLE IF NOT EXISTS db1.`tbl`(\n" +
                    "   `id` INT UNSIGNED AUTO_INCREMENT," +
                    "   PRIMARY KEY ( `id` )\n" +
                    ")ENGINE=InnoDB DEFAULT CHARSET=utf8;");

            ds0.setInstanceType("WRITE");
            ds1.setInstanceType("READ");


            execute(mycatConnection, CreateDataSourceHint
                    .create(ds0));

            execute(mycatConnection, CreateDataSourceHint
                    .create(ds1));


            execute(mycatConnection,
                    CreateClusterHint.create("prototype",
                            Arrays.asList("ds0"), Arrays.asList("ds1")));
            deleteData(readMysql, "db1", "tbl");
            execute(readMysql, "INSERT INTO db1.tbl \n" +
                    "(id)\n" +
                    "VALUES\n" +
                    " (1);");

            long now = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(1);

            while (System.currentTimeMillis() > now) {
                if (!hasData(mycatConnection, "db1", "tbl")) {
                    Assert.fail();
                }
            }
        }
    }

    /**
     * CreateTableHint targetname
     *
     * @throws Exception
     */
    @Test
    public void case13() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT_PSTMT);) {
            execute(mycatConnection, RESET_CONFIG);
            JdbcUtils.execute(mycatConnection, CreateDataSourceHint.create("ds0", DB2));
            JdbcUtils.execute(mycatConnection, CreateSchemaHint.create("mysql",
                    "prototype"));
            JdbcUtils.execute(mycatConnection, CreateTableHint.createNormal("mysql",
                    "testblob", "CREATE TABLE `testblob` (\n" +
                            "  `id` bigint(20) DEFAULT NULL,\n" +
                            "  `data` blob\n" +
                            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4", "ds0"));

            String explain = explain(mycatConnection, "select * from mysql.testblob");
            Assert.assertTrue(explain.contains("ds0"));
        }
    }

    @Test
    public void case14() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT_PSTMT);) {
            execute(mycatConnection, RESET_CONFIG);
            JdbcUtils.execute(mycatConnection, CreateSchemaHint.create("mysql",
                    "prototype"));
            JdbcUtils.execute(mycatConnection, "CREATE TABLE mysql.`testblob` (\n" +
                    "  `id` bigint(20) DEFAULT NULL,\n" +
                    "  `data` blob\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
            deleteData(mycatConnection, "mysql", "testblob");
            byte[] data = ByteBuffer.allocate(8).putLong(Long.MAX_VALUE).array();
            JdbcUtils.execute(mycatConnection, "INSERT INTO mysql.testblob \n" +
                    "(id,data)\n" +
                    "VALUES\n" +
                    " (1,?);", Arrays.asList(data));
            List<Map<String, Object>> maps = JdbcUtils.executeQuery(mycatConnection, "select * from mysql.testblob where id = 1", Collections.emptyList());
            byte[] data1 = (byte[]) maps.get(0).get("data");
            Assert.assertTrue(Arrays.equals(data, data1));
            System.out.println();
        }
    }

    private void testBlob(Connection mycatConnection, String createTableSQL) throws Exception {
        execute(mycatConnection, createTableSQL);

        deleteData(mycatConnection, "db1", "travelrecord");

        String text = "一二三四五六七八";
        byte[] testData = ByteUtil.getBytes(text, "UTF8");
        JdbcUtils.execute(mycatConnection, " INSERT INTO `db1`.`travelrecord` (`id`, `blob`) VALUES ('1', ?)", Arrays.asList(testData));
        Statement statement = mycatConnection.createStatement();
        ResultSet resultSet = statement.executeQuery("select `blob` from `db1`.`travelrecord` where id = 1");
        ResultSetMetaData metaData = resultSet.getMetaData();
        boolean next = resultSet.next();
        Object bytes = resultSet.getObject(1);
        Assert.assertEquals(text, new String((byte[]) bytes, StandardCharsets.UTF_8));
        System.out.println();
    }


    @Test
    public void case564() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT_PSTMT);) {

            execute(mycatConnection, RESET_CONFIG);
            JdbcUtils.execute(mycatConnection, CreateSchemaHint.create("mysql",
                    "prototype"));
            ShardingTableConfig shardingTableConfig = Json.decodeValue("{\n" +
                    "\t\t\t\"createTableSQL\":\"CREATE TABLE mysql.`cm_component_value` (`C_ID` varchar(32) NOT NULL,`C_REF_ID` varchar(32) DEFAULT NULL,`C_ORG_ID` bigint(20) DEFAULT NULL,`C_PATROL_ID` varchar(32) DEFAULT NULL,`C_CAMERA_ID` varchar(32) DEFAULT NULL,`C_DISTINGUISH_TYPE_ID` varchar(32) DEFAULT NULL ,`C_VALUE` varchar(64) DEFAULT NULL ,`C_STATE` varchar(32) DEFAULT '0' ,`C_TV_IMAGE_PATH` varchar(128) DEFAULT NULL ,`C_JSON_VALUE` varchar(512) DEFAULT NULL ,`C_IR_HOT_IMAGE_PATH` varchar(128) DEFAULT NULL ,`C_IR_VIDEO_IMAGE_PATH` varchar(128) DEFAULT NULL,`D_CREATE_TIME` datetime DEFAULT NULL ,`C_FAULT_LEVEL` varchar(32) DEFAULT 'UNKNOW',`C_COMPONENT_ID` varchar(32) NOT NULL ,`C_CONTENT` varchar(255) DEFAULT NULL ,`C_ERROR` varchar(32) DEFAULT NULL ,`C_PENDING_STATE` varchar(32) DEFAULT NULL ,`C_VALUE_SHOW` varchar(64) DEFAULT NULL ,`C_FILE_PATH` varchar(256) DEFAULT NULL ,`C_FTP_PATH` varchar(256) DEFAULT NULL ,`C_FILE_NAME_PATH` varchar(256) DEFAULT NULL,`FAULT_CONTENT` varchar(256) DEFAULT NULL ,`REVIEW_STATE` varchar(32) DEFAULT NULL ,`RECOGN_STATE` varchar(32) DEFAULT NULL ,`REVIEW_VALUE` varchar(32) DEFAULT NULL ,`CHECK_LEVEL` varchar(32) DEFAULT NULL ,`CHECK_TYPE` varchar(32) DEFAULT NULL ,`REVIEW_TIME` datetime DEFAULT NULL ,`PARAMETER_JSON` text ,`ALARM_NUM` varchar(8) DEFAULT NULL,`C_IS_UPLOAD` varchar(1) DEFAULT NULL,`C_VALUE_JSON` varchar(1024) DEFAULT NULL,`C_ORDER_ID` varchar(32) DEFAULT NULL,`D_UPLOAD_TIME` datetime DEFAULT NULL,PRIMARY KEY (`C_ID`),UNIQUE KEY `unique_index` (`c_ref_id`,`c_org_id`) USING BTREE,KEY `FK_cm_component_value_id` (`c_patrol_id`),KEY `FK_cm_com_value_distinguish_type_id` (`c_distinguish_type_id`),KEY `cm_component_value_ibfk_1` (`c_camera_id`),KEY `fk_0` (`c_patrol_id`),KEY `fk_1` (`c_org_id`),KEY `fk_2` (`c_camera_id`),KEY `fk_3` (`d_create_time`,`c_fault_level`,`c_state`,`c_org_id`),KEY `fk_4` (`c_patrol_id`,`c_state`),KEY `fk_5` (`c_patrol_id`,`c_state`,`c_fault_level`),KEY `fk_6` (`c_org_id`,`c_state`,`c_fault_level`),KEY `fk_8` (`c_org_id`,`c_component_id`),KEY `fk_9` (`c_org_id`,`d_create_time`),KEY `fk_10` (`d_create_time`,`c_fault_level`,`c_state`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;\",\n" +
                    "\t\t\t\"function\":{\n" +
                    "\t\t\t\t\"clazz\":\"io.mycat.router.mycat1xfunction.PartitionByMonth\",\n" +
                    "\t\t\t\t\"properties\":{\n" +
                    "\t\t\t\t\t\"beginDate\":\"2021-01-01 00:00:00\",\n" +
                    "\t\t\t\t\t\"endDate\":\"\",\n" +
                    "\t\t\t\t\t\"dateFormat\":\"yyyy-MM-dd hh:mm:ss\",\n" +
                    "\t\t\t\t\t\"columnName\":\"D_CREATE_TIME\"\n" +
                    "\t\t\t\t},\n" +
                    "\t\t\t\t\"ranges\":{}\n" +
                    "\t\t\t},\n" +
                    "\t\t\t\"partition\":{\n" +
                    "\t\t\t\t\"schemaNames\":\"yh\",\n" +
                    "\t\t\t\t\"tableNames\":\"cm_component_value_2021$01-12\",\n" +
                    "\t\t\t\t\"targetNames\":\"prototype\"\n" +
                    "\t\t\t}\n" +
                    "\t\t}\n", ShardingTableConfig.class);
            JdbcUtils.execute(mycatConnection, CreateTableHint.createSharding("mysql", "cm_component_value", shardingTableConfig));
            JdbcUtils.execute(mycatConnection, "use mysql");
            List<Map<String, Object>> maps = JdbcUtils.executeQuery(mycatConnection, "SELECT\n" +
                    "componentv0_.c_id AS c_id1_3_,\n" +
                    "componentv0_.alarm_num AS alarm_nu2_3_,\n" +
                    "componentv0_.c_camera_id AS c_camer32_3_,\n" +
                    "componentv0_.check_level AS check_le3_3_,\n" +
                    "componentv0_.check_type AS check_ty4_3_,\n" +
                    "componentv0_.c_component_id AS c_compo33_3_,\n" +
                    "componentv0_.c_content AS c_conten5_3_,\n" +
                    "componentv0_.d_create_time AS d_create6_3_,\n" +
                    "componentv0_.c_distinguish_type_id AS c_distin7_3_,\n" +
                    "componentv0_.c_error AS c_error8_3_,\n" +
                    "componentv0_.fault_content AS fault_co9_3_,\n" +
                    "componentv0_.c_fault_level AS c_fault10_3_,\n" +
                    "componentv0_.c_file_name_path AS c_file_11_3_,\n" +
                    "componentv0_.c_file_path AS c_file_12_3_,\n" +
                    "componentv0_.c_ftp_path AS c_ftp_p13_3_,\n" +
                    "componentv0_.c_ir_hot_image_path AS c_ir_ho14_3_,\n" +
                    "componentv0_.c_ir_video_image_path AS c_ir_vi15_3_,\n" +
                    "componentv0_.c_is_upload AS c_is_up16_3_,\n" +
                    "componentv0_.c_json_value AS c_json_17_3_,\n" +
                    "componentv0_.c_order_id AS c_order18_3_,\n" +
                    "componentv0_.c_org_id AS c_org_i34_3_,\n" +
                    "componentv0_.parameter_json AS paramet19_3_,\n" +
                    "componentv0_.c_patrol_id AS c_patro35_3_,\n" +
                    "componentv0_.c_pending_state AS c_pendi20_3_,\n" +
                    "componentv0_.recogn_state AS recogn_21_3_,\n" +
                    "componentv0_.c_ref_id AS c_ref_i22_3_,\n" +
                    "componentv0_.review_state AS review_23_3_,\n" +
                    "componentv0_.review_time AS review_24_3_,\n" +
                    "componentv0_.review_value AS review_25_3_,\n" +
                    "componentv0_.c_state AS c_state26_3_,\n" +
                    "componentv0_.c_tv_image_path AS c_tv_im27_3_,\n" +
                    "componentv0_.d_upload_time AS d_uploa28_3_,\n" +
                    "componentv0_.c_value AS c_value29_3_,\n" +
                    "componentv0_.c_value_json AS c_value30_3_,\n" +
                    "componentv0_.c_value_show AS c_value31_3_\n" +
                    "FROM\n" +
                    "cm_component_value componentv0_\n" +
                    "WHERE\n" +
                    "componentv0_.c_component_id = 'ff80808179e57c5a0179e61ccfc70c16'\n" +
                    "AND componentv0_.d_create_time >'2021-08-04 00:00:00'\n" +
                    "and componentv0_.d_create_time <'2021-08-05 23:59:59'\n" +
                    "ORDER BY\n" +
                    "componentv0_.d_create_time DESC\n" +
                    "LIMIT 1,100", Collections.emptyList());
            System.out.println();
        }
    }

    @Test
    public void case565() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT_PSTMT);) {

            execute(mycatConnection, RESET_CONFIG);
            execute(mycatConnection, "  CREATE DATABASE db1;");
            execute(mycatConnection, " /*+ mycat:createTable{\n" +
                    "  \"schemaName\":\"db1\",\n" +
                    "  \"shardingTable\":{\n" +
                    "    \"createTableSQL\":\"CREATE TABLE db1.`sharding` (\\n  `id` bigint NOT NULL AUTO_INCREMENT,\\n  `user_id` varchar(100) DEFAULT NULL,\\n  `create_time` date DEFAULT NULL,\\n  `fee` decimal(10,0) DEFAULT NULL,\\n  `days` int DEFAULT NULL,\\n  `blob` longblob,\\n  PRIMARY KEY (`id`),\\n  KEY `id` (`id`)\\n) ENGINE=InnoDB  DEFAULT CHARSET=utf8\",\n" +
                    "    \"function\":{\n" +
                    "      \"clazz\":\"io.mycat.router.mycat1xfunction.PartitionByHotDate\",\n" +
                    "      \"properties\":{\n" +
                    "        \"dateFormat\":\"yyyy-MM-dd\",\n" +
                    "        \"lastTime\":30,\n" +
                    "        \"partionDay\":30,\n" +
                    "        \"columnName\":\"create_time\"\n" +
                    "      }\n" +
                    "    },\n" +
                    "    \"partition\":{\n" +
                    "      \"schemaNames\":\"db1\",\n" +
                    "      \"tableNames\":\"sharding_$0-2\",\n" +
                    "      \"targetNames\":\"prototype\"\n" +
                    "    }\n" +
                    "  },\n" +
                    "  \"tableName\":\"sharding\"\n" +
                    "} */;");
            //execute(mycatConnection, "           INSERT INTO db1.sharding VALUES (null, 'test1', '2021-08-09', 2, 3, NULL);");
        }
    }

    @Test
    public void case15() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT_PSTMT)) {


            execute(mycatConnection, RESET_CONFIG);

            execute(mycatConnection, "DROP DATABASE db1");


            execute(mycatConnection, "CREATE DATABASE db1");

            execute(mycatConnection, CreateDataSourceHint
                    .create("ds0",
                            DB1));

            execute(mycatConnection,
                    CreateClusterHint.create("c0",
                            Arrays.asList("ds0"), Collections.emptyList()));


            JdbcUtils.execute(mycatConnection, "CREATE TABLE db1.`travelrecord` (\n" +
                    "  `id` bigint(20) NOT NULL KEY,\n" +
                    "  `user_id` varchar(100) CHARACTER SET utf8 DEFAULT NULL,\n" +
                    "  `traveldate` datetime(6) DEFAULT NULL,\n" +
                    "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                    "  `days` int(11) DEFAULT NULL,\n" +
                    "  `blob` longblob DEFAULT NULL\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                    "dbpartition by mod_hash(id) dbpartitions 16;");
            String explain = explain(mycatConnection, "select * from db1.travelrecord where id = 3300000000");
            Assert.assertTrue(explain.contains("MycatView(distribution=[[db1.travelrecord]], conditions=[=($0, ?0)])"));
            explain = explain(mycatConnection, "select * from db1.travelrecord where id in(3300000000,1101000990)");
            Assert.assertTrue(explain.contains("db1_0.travelrecord_0"));
            Assert.assertTrue(explain.contains("db1_14.travelrecord_0"));

            explain = explain(mycatConnection, "select * from db1.travelrecord where id in('3300000000','1101000990')");
            Assert.assertTrue(explain.contains("db1_0.travelrecord_0"));
            Assert.assertTrue(explain.contains("db1_14.travelrecord_0"));
            System.out.println();
        }
    }

    @Test
    public void case16() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT_PSTMT)) {


            execute(mycatConnection, RESET_CONFIG);

            execute(mycatConnection, "DROP DATABASE db1");


            execute(mycatConnection, "CREATE DATABASE db1");

            execute(mycatConnection, CreateDataSourceHint
                    .create("ds0",
                            DB1));

            execute(mycatConnection,
                    CreateClusterHint.create("c0",
                            Arrays.asList("ds0"), Collections.emptyList()));


            JdbcUtils.execute(mycatConnection, "CREATE TABLE db1.`travelrecord` (\n" +
                    "  `id` bigint(20) NOT NULL KEY,\n" +
                    "  `user_id` varchar(100) CHARACTER SET utf8 DEFAULT NULL,\n" +
                    "  `traveldate` datetime(6) DEFAULT NULL,\n" +
                    "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                    "  `days` int(11) DEFAULT NULL,\n" +
                    "  `blob` longblob DEFAULT NULL\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                    "dbpartition by mod_hash(user_id) dbpartitions 16;");
            String explain = explain(mycatConnection, "select * from db1.travelrecord where user_id = '3300000000'");
            Assert.assertTrue(explain.contains("db1_0.travelrecord_0"));
            explain = explain(mycatConnection, "select * from db1.travelrecord where user_id in('3300000000','1101000990')");
            Assert.assertTrue(explain.contains("db1_0.travelrecord_0"));
            Assert.assertTrue(explain.contains("db1_1.travelrecord_0"));

            explain = explain(mycatConnection, "select * from db1.travelrecord where user_id in('3300000000','1101000990')");
            Assert.assertTrue(explain.contains("db1_0.travelrecord_0"));
            Assert.assertTrue(explain.contains("db1_1.travelrecord_0"));
            System.out.println();
        }
    }

    @Test
    public void case17() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT)) {
            JdbcUtils.execute(mycatConnection, "use mysql");
            JdbcUtils.executeQuery(mycatConnection, "desc role_edges", Collections.emptyList());
            JdbcUtils.execute(mycatConnection, "use information_schema");
            JdbcUtils.executeQuery(mycatConnection, "desc `tables`", Collections.emptyList());
        }
    }


    @Test
    public void casePartitionData() throws Exception {

        ShardingBackEndTableInfoConfig shardingBackEndTableInfoConfig = new ShardingBackEndTableInfoConfig();
        List<IndexDataNode> indexDataNodes = Arrays.asList(
                new IndexDataNode("c0", "db1", "t1", 0, 0, 0),
                new IndexDataNode("c1", "db1", "t1", 1, 1, 1)
        );
        List<List> res = new ArrayList<>();
        for (IndexDataNode indexDataNode : indexDataNodes) {

            String targetName = indexDataNode.getTargetName();
            String schema = indexDataNode.getSchema();
            String table = indexDataNode.getTable();
            Integer dbIndex = indexDataNode.getDbIndex();
            Integer tableIndex = indexDataNode.getTableIndex();
            Integer index = indexDataNode.getIndex();

            ArrayList<String> objects = new ArrayList<>(3);
            objects.add(targetName);
            objects.add(schema);
            objects.add(table);
            objects.add(dbIndex.toString());
            objects.add(tableIndex.toString());
            objects.add(index.toString());

            res.add(objects);
        }
        shardingBackEndTableInfoConfig.setData(res);
        System.out.println(Json.encode(shardingBackEndTableInfoConfig));

        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT_PSTMT);) {

            execute(mycatConnection, RESET_CONFIG);
            execute(mycatConnection,
                    CreateClusterHint.create("c0",
                            Arrays.asList("prototypeDs"), Collections.emptyList()));
            execute(mycatConnection,
                    CreateClusterHint.create("c1",
                            Arrays.asList("prototypeDs"), Collections.emptyList()));

            execute(mycatConnection, "  CREATE DATABASE db1;");
            String sql = " /*+ mycat:createTable{\n" +
                    "  \"schemaName\":\"db1\",\n" +
                    "  \"shardingTable\":{\n" +
                    "    \"createTableSQL\":\"CREATE TABLE db1.`sharding` (\\n  `id` bigint NOT NULL AUTO_INCREMENT,\\n  `user_id` varchar(100) DEFAULT NULL,\\n  `create_time` date DEFAULT NULL,\\n  `fee` decimal(10,0) DEFAULT NULL,\\n  `days` int DEFAULT NULL,\\n  `blob` longblob,\\n  PRIMARY KEY (`id`),\\n  KEY `id` (`id`)\\n) ENGINE=InnoDB  DEFAULT CHARSET=utf8\",\n" +
                    "    \"function\":{\n" +
                    "      \"clazz\":\"io.mycat.router.mycat1xfunction.PartitionByHotDate\",\n" +
                    "      \"properties\":{\n" +
                    "        \"dateFormat\":\"yyyy-MM-dd\",\n" +
                    "        \"lastTime\":30,\n" +
                    "        \"partionDay\":30,\n" +
                    "        \"columnName\":\"create_time\"\n" +
                    "      }\n" +
                    "    },\n" +
                    "    \"partition\":{\n" +
                    "\"data\":[[\"c0\",\"db1\",\"t2\",\"0\",\"0\",\"0\"],[\"c1\",\"db1\",\"t2\",\"1\",\"1\",\"1\"]]" +
                    "    }\n" +
                    "  },\n" +
                    "  \"tableName\":\"sharding\"\n" +
                    "} */;";
            System.out.println(sql);
            execute(mycatConnection, sql);
            List<Map<String, Object>> maps = executeQuery(mycatConnection, ShowTopologyHint.create("db1", "sharding"));
            Assert.assertEquals("[{targetName=c0, schemaName=db1, tableName=t2, dbIndex=0, tableIndex=0, index=0}, {targetName=c1, schemaName=db1, tableName=t2, dbIndex=1, tableIndex=1, index=1}]",
                    maps.toString());
            System.out.println();
        }
    }

    @Test
    public void casePartitionData2() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT_PSTMT);) {

            execute(mycatConnection, RESET_CONFIG);
            execute(mycatConnection,
                    CreateClusterHint.create("c0",
                            Arrays.asList("prototypeDs"), Collections.emptyList()));
            execute(mycatConnection,
                    CreateClusterHint.create("c1",
                            Arrays.asList("prototypeDs"), Collections.emptyList()));

            execute(mycatConnection, "  CREATE DATABASE db1;");
            String sql = "\n" +
                    " /*+ mycat:createTable{\n" +
                    "  \"schemaName\":\"db1\",\n" +
                    "  \"shardingTable\":{\n" +
                    "  \n" +
                    "\"createTableSQL\":\"create table sharding(id int)\",\n" +
                    "  \n" +
                    "\"function\":{\n" +
                    "        \"properties\":{\n" +
                    "          \"dbNum\":2,\n" +
                    "          \"mappingFormat\":\"c${targetIndex}/db1_${dbIndex}/sharding_${tableIndex}\",\n" +
                    "          \"tableNum\":2,\n" +
                    "          \"tableMethod\":\"mod_hash(id)\",\n" +
                    "          \"storeNum\":2,\n" +
                    "          \"dbMethod\":\"mod_hash(id)\"\n" +
                    "      }\n" +
                    "    },\n" +
                    "    \"partition\":{\n" +
                    "\"data\":[[\"c0\",\"db1\",\"t2\",\"0\",\"0\",\"0\"],[\"c1\",\"db1\",\"t2\",\"1\",\"1\",\"1\"]]" +
                    "    }\n" +
                    "  },\n" +
                    "  \"tableName\":\"sharding\"\n" +
                    "} */;";
            System.out.println(sql);
            execute(mycatConnection, sql);
            List<Map<String, Object>> maps = executeQuery(mycatConnection, ShowTopologyHint.create("db1", "sharding"));
            Assert.assertEquals("[{targetName=c0, schemaName=db1, tableName=t2, dbIndex=0, tableIndex=0, index=0}, {targetName=c1, schemaName=db1, tableName=t2, dbIndex=1, tableIndex=1, index=1}]",
                    maps.toString());
            System.out.println();
        }
    }

    @Test
    public void case18() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);
             Connection mysqlConnection = getMySQLConnection(DB1)) {
            execute(mycatConnection, "CREATE DATABASE db1");
            JdbcUtils.execute(mycatConnection, "use db1");
            JdbcUtils.execute(mycatConnection, "create table if not exists char_test(c char(1));");

            Statement mycatStatement = mycatConnection.createStatement();
            Statement mysqlStatement = mysqlConnection.createStatement();

            ResultSet mycatResultSet = mycatStatement.executeQuery("select * from db1.char_test");
            ResultSetMetaData mycatMetaData = mycatResultSet.getMetaData();
            int mycatColumnType = mycatMetaData.getColumnType(1);

            ResultSet mysqlResultSet = mysqlStatement.executeQuery("select * from db1.char_test");
            ResultSetMetaData mysqlMetaData = mysqlResultSet.getMetaData();
            int mysqlColumnType = mysqlMetaData.getColumnType(1);
            Assert.assertEquals(mysqlColumnType, mycatColumnType);
        }

    }

    @Test
    public void case19() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT_PSTMT);) {
            execute(mycatConnection, RESET_CONFIG);
            execute(mycatConnection, "CREATE DATABASE db1");
            JdbcUtils.execute(mycatConnection, "use db1");
            JdbcUtils.execute(mycatConnection, "create table if not exists tinyint_test(`state` tinyint(1) DEFAULT '1');");
            deleteData(mycatConnection, "db1", "tinyint_test");
            JdbcUtils.execute(mycatConnection, "INSERT INTO `tinyint_test` ( `state`) VALUES (?)", Arrays.asList("1"));
            Statement statement = mycatConnection.createStatement();
            ResultSet resultSet = statement.executeQuery("select * from tinyint_test");
            resultSet.next();
            int anInt = resultSet.getInt(1);
            Assert.assertEquals(1, anInt);
            System.out.println();
        }

    }

    @Test
    public void case20() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT_PSTMT);) {
            execute(mycatConnection, RESET_CONFIG);
            execute(mycatConnection, "CREATE DATABASE db1");
            JdbcUtils.execute(mycatConnection, "use db1");
            JdbcUtils.execute(mycatConnection, "drop table if  exists int_test");
            JdbcUtils.execute(mycatConnection, "create table if not exists int_test(`state` int(11)) AUTO_INCREMENT=14132 DEFAULT ;");
            deleteData(mycatConnection, "db1", "int_test");
            JdbcUtils.execute(mycatConnection, "INSERT INTO `int_test` ( `state`) VALUES (?)", Arrays.asList("14130"));
            List<Map<String, Object>> maps = JdbcUtils.executeQuery(mycatConnection, "select * from int_test", Collections.emptyList());
            Statement statement = mycatConnection.createStatement();
            ResultSet resultSet = statement.executeQuery("select * from int_test");
            resultSet.next();
            int anInt = resultSet.getInt(1);
            Assert.assertEquals(14130, anInt);
            System.out.println();
        }

    }

    @Test
    public void case21() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT_PSTMT);) {
            List<Map<String, Object>> res0 = JdbcUtils.executeQuery(mycatConnection, "select ?", Arrays.asList(0.01d));
            List<Map<String, Object>> res1 = JdbcUtils.executeQuery(mycatConnection, "select ?", Arrays.asList(0.01f));
            List<Map<String, Object>> res2 = JdbcUtils.executeQuery(mycatConnection, "select ?", Arrays.asList(1));
            List<Map<String, Object>> res3 = JdbcUtils.executeQuery(mycatConnection, "select ?", Arrays.asList(1l));
            List<Map<String, Object>> res4 = JdbcUtils.executeQuery(mycatConnection, "select ?", Arrays.asList((byte) 1));
            List<Map<String, Object>> res5 = JdbcUtils.executeQuery(mycatConnection, "select ?", Arrays.asList((short) 1));

            String stringBlob = "{{";
            byte[] bytes = stringBlob.getBytes();
            List<Map<String, Object>> res6 = JdbcUtils.executeQuery(mycatConnection, "select ?", Arrays.asList(bytes));//not char ,char会被java序列化成数组
            List<Map<String, Object>> res7 = JdbcUtils.executeQuery(mycatConnection, "select ?", Arrays.asList(BigDecimal.valueOf(1)));
            List<Map<String, Object>> res8 = JdbcUtils.executeQuery(mycatConnection, "select ?", Arrays.asList(BigInteger.valueOf(1)));


            Assert.assertEquals("[{0.01=0.01}]", res0.toString());
            Assert.assertEquals("[{0.01=0.01}]", res1.toString());
            Assert.assertEquals("[{1=1}]", res2.toString());
            Assert.assertEquals("[{1=1}]", res3.toString());
            Assert.assertEquals("[{1=1}]", res4.toString());
            Assert.assertEquals("[{1=1}]", res5.toString());
            Assert.assertEquals("[{'{{'={{}]", res6.toString());
            Assert.assertEquals("[{'1'=1}]", res7.toString());
            Assert.assertEquals("[{1=1}]", res8.toString());
            System.out.println();
        }

    }


    @Test
    public void case596() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT_PSTMT);) {
            List<Map<String, Object>> maps = JdbcUtils.executeQuery(mycatConnection, "select REPLACE(UUID(),'-','')", Collections.emptyList());
            System.out.println();
        }

    }

    @Test
    public void case597() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);) {
            execute(mycatConnection, RESET_CONFIG);
            execute(mycatConnection, "CREATE DATABASE db1");
            JdbcUtils.execute(mycatConnection, "use db1");
            JdbcUtils.execute(mycatConnection, "drop table if  exists time_test");
            JdbcUtils.execute(mycatConnection, "create table if not exists time_test(`state` TIME) ");
            deleteData(mycatConnection, "db1", "time_test");
            LocalTime time = LocalTime.now();
            JdbcUtils.execute(mycatConnection, "INSERT INTO `time_test` ( `state`) VALUES (?)", Arrays.asList(time));
            List<Map<String, Object>> maps = JdbcUtils.executeQuery(mycatConnection, "select * from time_test", Collections.emptyList());
            Statement statement = mycatConnection.createStatement();
            ResultSet resultSet = statement.executeQuery("select * from time_test");
            resultSet.next();
            Object object = resultSet.getObject(1);
            Assert.assertTrue(object instanceof Time);
            System.out.println();
        }
    }


    @Test
    public void case597_1() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT_PSTMT);) {
            execute(mycatConnection, RESET_CONFIG);
            execute(mycatConnection, "CREATE DATABASE db1");
            JdbcUtils.execute(mycatConnection, "use db1");
            JdbcUtils.execute(mycatConnection, "drop table if  exists time_test");
            JdbcUtils.execute(mycatConnection, "create table if not exists time_test(`state` TIME) ");
            deleteData(mycatConnection, "db1", "time_test");
            LocalTime time = LocalTime.now();
            JdbcUtils.execute(mycatConnection, "INSERT INTO `time_test` ( `state`) VALUES (?)", Arrays.asList(time));
            List<Map<String, Object>> maps = JdbcUtils.executeQuery(mycatConnection, "select * from time_test", Collections.emptyList());
            Statement statement = mycatConnection.createStatement();
            ResultSet resultSet = statement.executeQuery("select * from time_test");
            resultSet.next();
            Object object = resultSet.getObject(1);
            Assert.assertTrue(object instanceof Time);
            System.out.println();
        }
    }

    @Test
    public void case597_2() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT_PSTMT);) {
            execute(mycatConnection, RESET_CONFIG);
            execute(mycatConnection, "CREATE DATABASE db1");
            JdbcUtils.execute(mycatConnection, "use db1");
            JdbcUtils.execute(mycatConnection, "drop table if  exists date_test");
            JdbcUtils.execute(mycatConnection, "create table if not exists date_test(`state` date) ");
            deleteData(mycatConnection, "db1", "date_test");
            LocalDate date = LocalDate.now();
            JdbcUtils.execute(mycatConnection, "INSERT INTO `date_test` ( `state`) VALUES (?)", Arrays.asList(date));
            List<Map<String, Object>> maps = JdbcUtils.executeQuery(mycatConnection, "select * from date_test", Collections.emptyList());
            Statement statement = mycatConnection.createStatement();
            ResultSet resultSet = statement.executeQuery("select * from date_test");
            resultSet.next();
            Object object = resultSet.getObject(1);
            Assert.assertTrue(object instanceof Date);
            System.out.println();
        }
    }

    @Test
    public void case597_3() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);) {
            execute(mycatConnection, RESET_CONFIG);
            execute(mycatConnection, "CREATE DATABASE db1");
            JdbcUtils.execute(mycatConnection, "use db1");
            JdbcUtils.execute(mycatConnection, "drop table if  exists date_test");
            JdbcUtils.execute(mycatConnection, "create table if not exists date_test(`state` date) ");
            deleteData(mycatConnection, "db1", "date_test");
            LocalDate date = LocalDate.now();
            JdbcUtils.execute(mycatConnection, "INSERT INTO `date_test` ( `state`) VALUES (?)", Arrays.asList(date));
            List<Map<String, Object>> maps = JdbcUtils.executeQuery(mycatConnection, "select * from date_test", Collections.emptyList());
            Statement statement = mycatConnection.createStatement();
            ResultSet resultSet = statement.executeQuery("select * from date_test");
            resultSet.next();
            Object object = resultSet.getObject(1);
            Assert.assertTrue(object instanceof Date);
            System.out.println();
        }
    }

    @Test
    public void case597_4() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);) {
            execute(mycatConnection, RESET_CONFIG);
            execute(mycatConnection, "CREATE DATABASE db1");
            JdbcUtils.execute(mycatConnection, "use db1");
            JdbcUtils.execute(mycatConnection, "drop table if  exists datetime_test");
            JdbcUtils.execute(mycatConnection, "create table if not exists datetime_test(`state` DATETIME) ");
            deleteData(mycatConnection, "db1", "datetime_test");
            LocalDateTime dateTime = LocalDateTime.now();
            JdbcUtils.execute(mycatConnection, "INSERT INTO `datetime_test` ( `state`) VALUES (?)", Arrays.asList(dateTime));
            List<Map<String, Object>> maps = JdbcUtils.executeQuery(mycatConnection, "select * from datetime_test", Collections.emptyList());
            Statement statement = mycatConnection.createStatement();
            ResultSet resultSet = statement.executeQuery("select * from datetime_test");
            resultSet.next();
            Object object = resultSet.getObject(1);
            Assert.assertTrue(object instanceof Timestamp);
            System.out.println();
        }
    }

    @Test
    public void case597_5() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT_PSTMT);) {
            execute(mycatConnection, RESET_CONFIG);
            execute(mycatConnection, "CREATE DATABASE db1");
            JdbcUtils.execute(mycatConnection, "use db1");
            JdbcUtils.execute(mycatConnection, "drop table if  exists datetime_test");
            JdbcUtils.execute(mycatConnection, "create table if not exists datetime_test(`state` DATETIME) ");
            deleteData(mycatConnection, "db1", "datetime_test");
            LocalDateTime dateTime = LocalDateTime.now();
            JdbcUtils.execute(mycatConnection, "INSERT INTO `datetime_test` ( `state`) VALUES (?)", Arrays.asList(dateTime));
            List<Map<String, Object>> maps = JdbcUtils.executeQuery(mycatConnection, "select * from datetime_test", Collections.emptyList());
            Statement statement = mycatConnection.createStatement();
            ResultSet resultSet = statement.executeQuery("select * from datetime_test");
            resultSet.next();
            Object object = resultSet.getObject(1);
            Assert.assertTrue(object instanceof Timestamp);
            System.out.println();
        }
    }

    @Test
    public void case599() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT_PSTMT);) {
            List<Map<String, Object>> maps1 = executeQuery(mycatConnection, "select DATE_SUB(NOW(), INTERVAL 1 MONTH)");
            List<Map<String, Object>> maps2 = executeQuery(mycatConnection, "select DATE_SUB(NOW(), INTERVAL '1' MONTH)");
            String s1 = maps1.get(0).values().toString();
            s1 = s1.substring(0, s1.length() - 2);

            String s2 = maps2.get(0).values().toString();
            s2 = s2.substring(0, s2.length() - 2);
            Assert.assertEquals(s1, s2);
            System.out.println();
        }
    }

    @Test
    public void case599_1() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT_PSTMT);) {
            List<Map<String, Object>> maps1 = executeQuery(mycatConnection, "select DATE_SUB(NOW(), INTERVAL 30 MINUTE)");
            System.out.println();
        }
    }

    @Test
    public void case604() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT_PSTMT);) {
            List<Map<String, Object>> maps1 = executeQuery(mycatConnection, "select DATABASE()");
            List<Map<String, Object>> maps2 = executeQuery(mycatConnection, "/*+mycat:schema=ds2*/select DATABASE()");
            List<Map<String, Object>> maps3 = executeQuery(mycatConnection, "select DATABASE()");
            Assert.assertEquals(maps1, maps3);
            Assert.assertTrue(maps2.toString().contains("ds2"));
            Assert.assertNotEquals(maps1, maps2);
            System.out.println();
        }
    }

    @Test
    public void case605() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);) {
            execute(mycatConnection, RESET_CONFIG);
            addC0(mycatConnection);
            execute(mycatConnection, "create database db1");
            execute(mycatConnection, "use db1");
            execute(mycatConnection, "CREATE TABLE `float_test` (\n" +
                    "  `value` float DEFAULT '0' \n" +
                    ") ENGINE=InnoDB AUTO_INCREMENT=11 DEFAULT CHARSET=utf8;");

            deleteData(mycatConnection, "db1", "float_test");
            execute(mycatConnection, "\n" +
                    "INSERT INTO `db1`.`float_test`(`value`) VALUES (20.2);\n");
            execute(mycatConnection, "\n" +
                    "INSERT INTO `db1`.`float_test`(`value`) VALUES (500);\n");
            List<Map<String, Object>> maps = executeQuery(mycatConnection, "select * from db1.float_test;");
            Assert.assertTrue(maps.toString().equals("[{value=20.2}, {value=500.0}]"));
        }
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT_PSTMT);) {
            execute(mycatConnection, RESET_CONFIG);
            addC0(mycatConnection);
            execute(mycatConnection, "create database db1");
            execute(mycatConnection, "use db1");
            execute(mycatConnection, "CREATE TABLE `float_test` (\n" +
                    "  `value` float DEFAULT '0' \n" +
                    ") ENGINE=InnoDB AUTO_INCREMENT=11 DEFAULT CHARSET=utf8;");

            deleteData(mycatConnection, "db1", "float_test");
            execute(mycatConnection, "\n" +
                    "INSERT INTO `db1`.`float_test`(`value`) VALUES (20.2);\n");
            execute(mycatConnection, "\n" +
                    "INSERT INTO `db1`.`float_test`(`value`) VALUES (500);\n");
            List<Map<String, Object>> maps = executeQuery(mycatConnection, "select * from db1.float_test;");
            Assert.assertTrue(maps.toString().equals("[{value=20.2}, {value=500.0}]"));
        }
    }


    @Test
    public void case6052() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);) {
            execute(mycatConnection, RESET_CONFIG);
            addC0(mycatConnection);
            execute(mycatConnection, "create database db1");
            execute(mycatConnection, "use db1");
            execute(mycatConnection, "CREATE TABLE `float_test` (\n" +
                    "  `value` float DEFAULT '0' \n" +
                    ") ENGINE=InnoDB AUTO_INCREMENT=11 DEFAULT CHARSET=utf8  dbpartition by mod_hash(value) dbpartitions 16;");

            deleteData(mycatConnection, "db1", "float_test");
            execute(mycatConnection, "\n" +
                    "INSERT INTO `db1`.`float_test`(`value`) VALUES (20.2);\n");
            execute(mycatConnection, "\n" +
                    "INSERT INTO `db1`.`float_test`(`value`) VALUES (500);\n");
            List<Map<String, Object>> maps = executeQuery(mycatConnection, "select * from db1.float_test;");
            Assert.assertTrue(maps.toString().equals("[{value=20.2}, {value=500.0}]"));
            System.out.println();
        }
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT_PSTMT);) {
            execute(mycatConnection, RESET_CONFIG);
            addC0(mycatConnection);
            execute(mycatConnection, "create database db1");
            execute(mycatConnection, "use db1");
            execute(mycatConnection, "CREATE TABLE `float_test` (\n" +
                    "  `value` float DEFAULT '0' \n" +
                    ") ENGINE=InnoDB AUTO_INCREMENT=11 DEFAULT CHARSET=utf8  dbpartition by mod_hash(value) dbpartitions 16;");

            deleteData(mycatConnection, "db1", "float_test");
            execute(mycatConnection, "\n" +
                    "INSERT INTO `db1`.`float_test`(`value`) VALUES (20.2);\n");
            execute(mycatConnection, "\n" +
                    "INSERT INTO `db1`.`float_test`(`value`) VALUES (500);\n");
            List<Map<String, Object>> maps = executeQuery(mycatConnection, "select * from db1.float_test;");
            Assert.assertTrue(maps.toString().equals("[{value=20.2}, {value=500.0}]"));
            execute(mycatConnection, "\n" +
                    "INSERT INTO `db1`.`float_test`(`value`) VALUES (true),(false);\n");
            System.out.println();
        }
    }

    @Test
    public void case611() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);) {
            execute(mycatConnection, RESET_CONFIG);
            addC0(mycatConnection);
            execute(mycatConnection, "create database db1");
            execute(mycatConnection, "use db1");
            execute(mycatConnection, "CREATE TABLE `tag` (\n" +
                    "  `id` bigint(20) NOT NULL,\n" +
                    "  `name` varchar(255) DEFAULT NULL,\n" +
                    "  `description` varchar(255) DEFAULT NULL,\n" +
                    "  `projectid` bigint(20) DEFAULT NULL,\n" +
                    "  `masterid` bigint(20) DEFAULT NULL,\n" +
                    "  `appid` varchar(255) DEFAULT NULL,\n" +
                    "  `user` varchar(255) DEFAULT NULL,\n" +
                    "  `tag` varchar(255) DEFAULT NULL,\n" +
                    "  `ct` datetime(6) DEFAULT NULL,\n" +
                    "  `ut` datetime(6) DEFAULT NULL,\n" +
                    "  `deleted` tinyint(1) DEFAULT NULL,\n" +
                    "  `userid` int(11) DEFAULT NULL,\n" +
                    "  PRIMARY KEY (`id`) USING BTREE\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=COMPACT  dbpartition by mod_hash(projectid) dbpartitions 16;");

            deleteData(mycatConnection, "db1", "tag");
            execute(mycatConnection, "\n" +
                    "INSERT INTO `db1`.`tag`(`id`,`user`,projectid,masterid) VALUES (20,'`````````',1,33538260487639040);\n");
            List<Map<String, Object>> maps = executeQuery(mycatConnection, "SELECT `id`,`name`,`description`,`projectid`,`masterid`,`appid`,`user`,`userid`,`tag`,`ct`,`ut`,`deleted` FROM `tag` WHERE `masterid`= 33538260487639040 ORDER BY `ct` DESC;");
            Assert.assertTrue(maps.toString().contains("`````````"));
        }
    }

    @Test
    public void case627() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);) {
            execute(mycatConnection, RESET_CONFIG);
            addC0(mycatConnection);
            execute(mycatConnection, "create database db1");
            execute(mycatConnection, "use db1");
            execute(mycatConnection, "CREATE TABLE `travelrecord` (\n" +
                    "  `id` bigint(20) NOT NULL KEY,\n" +
                    "  `user_id` varchar(100) CHARACTER SET utf8 DEFAULT NULL,\n" +
                    "  `traveldate` datetime(6) DEFAULT NULL,\n" +
                    "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                    "  `days` int(11) DEFAULT NULL,\n" +
                    "  `blob` longblob DEFAULT NULL\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n"
            );


            deleteData(mycatConnection, "db1", "travelrecord");
            byte[] bytes = "<?xml version='1.0' encoding='UTF-8'?>\n<definitions xmlns=\"http://www.omg.org/spec/BPMN/20100524/MODEL\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:activiti=\"http://activiti.org/bpmn\" xmlns:bpmndi=\"http://www.omg.org/spec/BPMN/20100524/DI\" xmlns:omgdc=\"http://www.omg.org/spec/DD/20100524/DC\" xmlns:omgdi=\"http://www.omg.org/spec/DD/20100524/DI\" typeLanguage=\"http://www.w3.org/2001/XMLSchema\" expressionLanguage=\"http://www.w3.org/1999/XPath\" targetNamespace=\"http://www.activiti.org/processdef\">".getBytes();
            JdbcUtils.execute(mycatConnection,
                    "INSERT INTO `db1`.`travelrecord`(`id`,`blob`) VALUES (20,?)", Arrays.asList(bytes));
            List<Map<String, Object>> maps = executeQuery(mycatConnection, "SELECT `blob` from db1.travelrecord");
            Object next = maps.get(0).values().iterator().next();
            if (next instanceof String) {
                Assert.assertTrue(next.toString().equals(new String(bytes)));
            } else {
                Assert.assertTrue(Arrays.equals(bytes, (byte[]) next));
            }

        }
    }

    @Test
    public void caseSelectSessionValueInt() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);) {
            List<Map<String, Object>> maps1 = executeQuery(mycatConnection, "select @@max_allowed_packet; ");
            Number next = (Number) maps1.get(0).values().iterator().next();

            maps1 = executeQuery(mycatConnection, "SELECT @@innodb_file_per_table =1;");
            next = (Number) maps1.get(0).values().iterator().next();

            maps1 = executeQuery(mycatConnection, "SELECT @@FOREIGN_KEY_CHECKS");
            next = (Number) maps1.get(0).values().iterator().next();
        }
    }
}
