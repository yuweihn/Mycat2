package io.mycat.datasource.jdbc;

import io.mycat.MycatConnection;
import io.mycat.TransactionSession;
import io.mycat.api.collector.UpdateRowIteratorResponse;
import io.mycat.beans.resultset.MycatUpdateResponse;
import io.mycat.plug.PlugRuntime;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.replica.PhysicsInstanceImpl;
import io.mycat.replica.ReplicaSelectorRuntime;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author Junwen Chen
 **/
public class TransactionSessionUtil {

    public static MycatConnection getConnectionByReplicaName(TransactionSession transactionSession, String replicaName, boolean update, String strategy) {
        return getDefaultConnection(replicaName, update, strategy, transactionSession);
    }

    public static MycatConnection getDefaultConnection(String replicaName, boolean update, String strategy, TransactionSession transactionSession) {
        LoadBalanceStrategy loadBalanceByBalanceName = PlugRuntime.INSTANCE.getLoadBalanceByBalanceName(strategy);
        PhysicsInstanceImpl datasource = ReplicaSelectorRuntime.INSTANCE.getDatasourceByReplicaName(replicaName, update, loadBalanceByBalanceName);
        String name;
        if (datasource == null) {
            name = replicaName;
        } else {
            name = datasource.getName();
        }
        return transactionSession.getConnection(Objects.requireNonNull(name));
    }


    public static MycatUpdateResponse executeUpdateByReplicaName(TransactionSession transactionSession, String replicaName,
                                                                 String sql,
                                                                 boolean needGeneratedKeys,
                                                                 String strategy) {
        MycatConnection connection = getConnectionByReplicaName(transactionSession, replicaName, true, strategy);
        return connection.executeUpdate(sql, needGeneratedKeys, transactionSession.getServerStatus());
    }

    public static MycatUpdateResponse executeUpdate(TransactionSession transactionSession, String datasource, String sql, boolean needGeneratedKeys) {
        MycatConnection connection = transactionSession.getConnection(datasource);
        return connection.executeUpdate(sql, needGeneratedKeys, transactionSession.getServerStatus());
    }

    public static UpdateRowIteratorResponse executeUpdateByDatasouce(TransactionSession transactionSession, Map<String, List<String>> map, boolean needGeneratedKeys, boolean global) {
        int lastId = 0;
        int count = 0;
        int serverStatus = 0;
        int sqlCount = 0;
        for (Map.Entry<String, List<String>> backendTableInfoStringEntry : map.entrySet()) {
            for (String s : backendTableInfoStringEntry.getValue()) {
                sqlCount++;
                MycatUpdateResponse mycatUpdateResponse = executeUpdate(transactionSession, backendTableInfoStringEntry.getKey(), s, needGeneratedKeys);
                long lastInsertId = mycatUpdateResponse.getLastInsertId();
                long updateCount = mycatUpdateResponse.getUpdateCount();
                lastId = Math.max((int) lastInsertId, lastId);
                count += updateCount;
                serverStatus = mycatUpdateResponse.serverStatus();
            }
        }
        return new UpdateRowIteratorResponse(global ? count / sqlCount : count, lastId, serverStatus);
    }
}