package io.mycat.datasource.jdbc;

import io.mycat.MycatException;
import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.config.ConfigEnum;
import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.config.datasource.JdbcDriverRootConfig;
import io.mycat.config.datasource.MasterIndexesRootConfig;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.config.datasource.ReplicasRootConfig;
import io.mycat.config.schema.DataNodeConfig;
import io.mycat.config.schema.DataNodeRootConfig;
import io.mycat.proxy.ProxyRuntime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class GridRuntime {

  final ProxyRuntime proxyRuntime;
  final Map<String, JdbcReplica> jdbcReplicaMap = new HashMap<>();
  final Map<String, JdbcDataNode> jdbcDataNodeMap = new HashMap<>();


  public GridRuntime(ProxyRuntime proxyRuntime) throws Exception {
    this.proxyRuntime = proxyRuntime;
    ReplicasRootConfig dsConfig = proxyRuntime.getConfig(ConfigEnum.DATASOURCE);
    MasterIndexesRootConfig replicaIndexConfig = proxyRuntime.getConfig(ConfigEnum.REPLICA_INDEX);
    JdbcDriverRootConfig jdbcDriverRootConfig = proxyRuntime.getConfig(ConfigEnum.JDBC_DRIVER);
    String datasourceProviderClass = jdbcDriverRootConfig.getDatasourceProviderClass();
    Objects.requireNonNull(datasourceProviderClass);
    DatasourceProvider datasourceProvider;
    try {
      datasourceProvider = (DatasourceProvider) Class.forName(datasourceProviderClass).newInstance();
    } catch (Exception e) {
      throw new MycatException("can not load datasourceProvider:{}", datasourceProviderClass);
    }
    initJdbcReplica(dsConfig, replicaIndexConfig, jdbcDriverRootConfig.getJdbcDriver(),datasourceProvider);
    DataNodeRootConfig dataNodeRootConfig = proxyRuntime.getConfig(ConfigEnum.DATANODE);
    initJdbcDataNode(dataNodeRootConfig);
  }

  public JdbcSession getJdbcSessionByDataNodeName(String dataNodeName,
      MySQLIsolation isolation,
      MySQLAutoCommit autoCommit,
      JdbcDataSourceQuery query) {
    JdbcDataNode  jdbcDataNode = jdbcDataNodeMap.get(dataNodeName);
    JdbcReplica replica = jdbcDataNode.getReplica();
    JdbcSession session =replica
        .getJdbcSessionByBalance(query);
    session.sync(isolation, autoCommit);
    return session;
  }


  private void initJdbcReplica(ReplicasRootConfig replicasRootConfig,
      MasterIndexesRootConfig replicaIndexConfig, Map<String, String> jdbcDriverMap,
      DatasourceProvider datasourceProvider)
      throws Exception {
    Map<String, String> masterIndexes = replicaIndexConfig.getMasterIndexes();
    Objects.requireNonNull(jdbcDriverMap, "jdbcDriver.yml is not existed.");
    for (String valve : jdbcDriverMap.values()) {
      Class.forName(valve);
    }
    if (replicasRootConfig != null && replicasRootConfig.getReplicas() != null
        && !replicasRootConfig.getReplicas().isEmpty()) {
      for (ReplicaConfig replicaConfig : replicasRootConfig.getReplicas()) {
        Set<Integer> replicaIndexes = ProxyRuntime.getReplicaIndexes(masterIndexes, replicaConfig);
        List<JdbcDataSource> jdbcDatasourceList = getJdbcDatasourceList(replicaConfig);
        if (jdbcDatasourceList.isEmpty()) {
          return;
        }
        JdbcReplica jdbcReplica = new JdbcReplica(proxyRuntime, jdbcDriverMap, replicaConfig,
            replicaIndexes,jdbcDatasourceList,datasourceProvider);
        jdbcReplicaMap.put(jdbcReplica.getName(), jdbcReplica);
      }
    }
  }

  private void initJdbcDataNode(DataNodeRootConfig config) {
    if (config != null && config.getDataNodes() != null) {
      List<DataNodeConfig> dataNodes = config.getDataNodes();
      for (DataNodeConfig dataNode : dataNodes) {
        jdbcDataNodeMap.put(dataNode.getName(),
            new JdbcDataNode(jdbcReplicaMap.get(dataNode.getReplica()), dataNode));
      }
    }
  }
  public static List<JdbcDataSource> getJdbcDatasourceList(ReplicaConfig replicaConfig) {
    List<DatasourceConfig> mysqls = replicaConfig.getMysqls();
    if (mysqls == null) {
      return Collections.emptyList();
    }
    List<JdbcDataSource> datasourceList = new ArrayList<>();
    for (int index = 0; index < mysqls.size(); index++) {
      DatasourceConfig datasourceConfig = mysqls.get(index);
      if (datasourceConfig.getDbType() != null) {
        datasourceList.add(new JdbcDataSource(index, datasourceConfig));
      }
    }
    return datasourceList;
  }

}