package cn.mycat.vertx.xa.impl;

import cn.mycat.vertx.xa.ImmutableCoordinatorLog;
import cn.mycat.vertx.xa.MySQLManager;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlConnection;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class LocalXaMemoryRepositoryImpl extends MemoryRepositoryImpl {
    private final static Logger LOGGER = LoggerFactory.getLogger(LocalXaMemoryRepositoryImpl.class);
    private Supplier<MySQLManager> mySQLManagerSupplier;
    public static final String database = "mycat";
    public static final String tableName = "xa_log";


    public static LocalXaMemoryRepositoryImpl createLocalXaMemoryRepository(Supplier<MySQLManager> mySQLManagerSupplier) {
        LocalXaMemoryRepositoryImpl localXaMemoryRepository = new LocalXaMemoryRepositoryImpl(mySQLManagerSupplier);
         localXaMemoryRepository.init();
         return localXaMemoryRepository;
    }

    private LocalXaMemoryRepositoryImpl(Supplier<MySQLManager> mySQLManagerSupplier) {
        this.mySQLManagerSupplier = mySQLManagerSupplier;
    }

    /**
     * 不理会是否创建,如果创建不成功,需要人工创建
     * @param dataSource
     * @return
     */
    public static Future<Void> tryCreateLogTable(SqlConnection dataSource) {
        String createDatabaseSQL = "create database if not exists `" + database + "`";
        String createTableSQL = "create table if not exists `" + database + "`." + "`" + tableName + "`"
                + "(`xid` bigint PRIMARY KEY NOT NULL"+
                ") ENGINE=InnoDB";
        return dataSource.query(createDatabaseSQL).execute().mapEmpty().flatMap(o -> dataSource.query(createTableSQL).execute().mapEmpty()).otherwise(throwable -> {
            return null;
        }).mapEmpty();
    }

    @Override
    public Future<Void> init() {
      return super.init();
    }

    @Override
    public Future<Void> close() {
        return super.close();
    }

    @Override
    public void remove(String xid) {
        super.remove(xid);
    }
}
