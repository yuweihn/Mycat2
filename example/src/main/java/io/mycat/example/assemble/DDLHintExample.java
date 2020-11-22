package io.mycat.example.assemble;

import io.mycat.hint.*;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DDLHintExample extends AssembleExample {

    @Override
    protected void initCluster(Connection mycatConnection) throws SQLException {
        execute(mycatConnection,
                "/*+ mycat:createTable{\"name\":\"dw0\",\"url\":\"jdbc:mysql://127.0.0.1:3306\",\"user\":\"root\",\"password\":\"123456\"} */;");

    }

    @Test
    public void testCreateTable() throws SQLException {

    }


    @Test
    public void testAddDatasource() throws SQLException {
        Connection mycat = getMySQLConnection(8066);
        String dsName = "newDs";
        execute(mycat, DropDataSourceHint.create(dsName));
        Assert.assertTrue(
                !executeQuery(mycat, "/*+ mycat:showDataSources{} */")
                        .toString().contains(dsName));
        execute(mycat, CreateDataSourceHint
                .create(dsName,
                        "jdbc:mysql://127.0.0.1:3306"));
        Assert.assertTrue(
                executeQuery(mycat, "/*+ mycat:showDataSources{} */")
                        .toString().contains("newDs"));
        execute(mycat, DropDataSourceHint.create(dsName));
        Assert.assertTrue(
                !executeQuery(mycat, "/*+ mycat:showDataSources{} */")
                        .toString().contains(dsName));
    }


    @Test
    public void testAddCluster() throws SQLException {
        String clusterName = "testAddCluster";
        Connection mycat = getMySQLConnection(8066);
        execute(mycat, DropClusterHint.create(clusterName));
        Assert.assertTrue(
                !executeQuery(mycat, "/*+ mycat:showClusters{} */")
                .toString().contains(clusterName));
        execute(mycat, CreateDataSourceHint
                .create("newDs",
                        "jdbc:mysql://127.0.0.1:3306"));
        execute(mycat, CreateClusterHint.create(clusterName, Arrays.asList("newDs"), Collections.emptyList()));
        Assert.assertTrue(
                executeQuery(mycat, "/*+ mycat:showClusters{} */")
                .toString().contains(clusterName));
        execute(mycat, DropClusterHint.create(clusterName));

        Assert.assertTrue(
                !executeQuery(mycat, "/*+ mycat:showClusters{} */")
                        .toString().contains(clusterName));
        execute(mycat, DropDataSourceHint
                .create("newDs"));

    }

    @Test
    public void testAddSchema() throws SQLException {
        Connection mycat = getMySQLConnection(8066);
        Connection mysql = getMySQLConnection(3306);
        String schemaName = "testAddSchema";
        String tableName = "testTable";
        execute(mysql, "create database  if not exists " + schemaName);
        execute(mysql, "create table if not exists testAddSchema." +
                tableName +
                " (id bigint) ");

        execute(mycat, CreateSchemaHint.create(schemaName));
        List<Map<String, Object>> shouldContainsTestAddSchema = executeQuery(mycat, "show databases");
        Assert.assertTrue(shouldContainsTestAddSchema.toString().contains(schemaName));
        List<Map<String, Object>> shouldContainsTable = executeQuery(mycat, "show tables from " + schemaName);
        Assert.assertTrue(shouldContainsTable.toString().contains(schemaName));
        execute(mycat, "drop database " + schemaName);
        List<Map<String, Object>> shouldNotContainsTestAddSchema = executeQuery(mycat, "show databases");
        Assert.assertFalse(shouldNotContainsTestAddSchema.toString().contains(schemaName));
    }
}
