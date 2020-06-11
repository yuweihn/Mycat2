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
package io.mycat.metadata;

import io.mycat.config.ShardingQueryRootConfig;
import io.mycat.config.ShardingTableConfig;
import io.mycat.config.SharingFuntionRootConfig;
import io.mycat.SimpleColumnInfo;
import io.mycat.util.YamlUtil;

import java.util.*;

/**
 * @author Junwen Chen
 **/
public class MetadataManagerBuilder {

    public static void exampleBuild(MetadataManager m) {
        m.addSchema("db1","defaultDs");
        ShardingQueryRootConfig.BackEndTableInfoConfig.BackEndTableInfoConfigBuilder builder = backEndBuilder();
        List<ShardingQueryRootConfig.BackEndTableInfoConfig> tableInfos = Arrays.asList(
                backEndBuilder().targetName("defaultDatasourceName").schemaName("db1").tableName("TRAVELRECORD").build(),
                backEndBuilder().targetName("defaultDatasourceName").schemaName("db1").tableName("TRAVELRECORD2").build(),
                backEndBuilder().targetName("defaultDatasourceName").schemaName("db1").tableName("TRAVELRECORD3").build(),

                backEndBuilder().targetName("defaultDatasourceName").schemaName("db2").tableName("TRAVELRECORD").build(),
                backEndBuilder().targetName("defaultDatasourceName").schemaName("db2").tableName("TRAVELRECORD2").build(),
                backEndBuilder().targetName("defaultDatasourceName").schemaName("db2").tableName("TRAVELRECORD3").build(),

                backEndBuilder().targetName("defaultDatasourceName").schemaName("db3").tableName("TRAVELRECORD").build(),
                backEndBuilder().targetName("defaultDatasourceName").schemaName("db3").tableName("TRAVELRECORD2").build(),
                backEndBuilder().targetName("defaultDatasourceName").schemaName("db3").tableName("TRAVELRECORD3").build()
        );

        Map<String, String> properties = new HashMap<>();
        properties.put("partitionCount", "2,1");
        properties.put("partitionLength", "256,512");

        ShardingTableConfig build = ShardingTableConfig.builder()
                .columns(Arrays.asList(ShardingQueryRootConfig.Column.builder()
                        .columnName("id").function(SharingFuntionRootConfig.ShardingFuntion.builder().name("partitionByLong")
                                .clazz("io.mycat.router.function.PartitionByLong").properties(properties).ranges(Collections.emptyMap())
                                .build()).shardingType(SimpleColumnInfo.ShardingType.NATURE_DATABASE_TABLE.name()).build()))
                .createTableSQL("CREATE TABLE `travelrecord` (\n" +
                        "  `id` bigint(20) NOT NULL,\n" +
                        "  `user_id` varchar(100) CHARACTER SET utf8 DEFAULT NULL,\n" +
                        "  `traveldate` date DEFAULT NULL,\n" +
                        "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                        "  `days` int(11) DEFAULT NULL,\n" +
                        "  `blob` longblob DEFAULT NULL,\n" +
                        "  `d` double DEFAULT NULL\n" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;")
                .build();
        System.out.println(YamlUtil.dump(build));
        m.addTable("db1", "travelrecord",build ,tableInfos,null);
    }

    public static ShardingQueryRootConfig.BackEndTableInfoConfig.BackEndTableInfoConfigBuilder backEndBuilder() {
        return ShardingQueryRootConfig.BackEndTableInfoConfig.builder();
    }
}