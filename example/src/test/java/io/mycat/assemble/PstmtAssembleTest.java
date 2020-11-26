package io.mycat.assemble;

import com.alibaba.druid.pool.DruidDataSource;
import com.mysql.cj.jdbc.MysqlDataSource;
import lombok.SneakyThrows;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.util.function.Function;

@NotThreadSafe
public class PstmtAssembleTest extends AssembleTest  {

    @Override
    public Connection getMySQLConnection(int port) throws Exception {
        return dsMap.computeIfAbsent(port, new Function<Integer, DruidDataSource>() {
            @Override
            @SneakyThrows
            public DruidDataSource apply(Integer integer) {
                String username = "root";
                String password = "123456";
                DruidDataSource dataSource = new DruidDataSource();
                dataSource.setUrl("jdbc:mysql://127.0.0.1:" +
                        port + "/?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true&useServerPrepStmts=true");
                dataSource.setUsername(username);
                dataSource.setPassword(password);
                return dataSource;
            }
        }).getConnection();

    }

    @Test
    @Override
    public void testTranscationFail2() throws Exception {
        super.testTranscationFail2();
    }
    @Test
    @Override
    public void testTranscationFail() throws Exception {
        super.testTranscationFail();
    }
    @Test
    @Override
    public void testBase() throws Exception {
        super.testBase();
    }
    @Test
    @Override
    public void testInfoFunction() throws Exception {
        super.testInfoFunction();
    }
}
