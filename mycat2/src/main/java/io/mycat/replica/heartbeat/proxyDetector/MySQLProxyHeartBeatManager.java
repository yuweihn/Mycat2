package io.mycat.replica.heartbeat.proxyDetector;

import io.mycat.config.ConfigEnum;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.config.datasource.ReplicaConfig.RepSwitchTypeEnum;
import io.mycat.config.heartbeat.HeartbeatConfig;
import io.mycat.config.heartbeat.HeartbeatRootConfig;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.replica.MySQLDataSourceEx;
import io.mycat.replica.MySQLDatasource;
import io.mycat.replica.heartbeat.DatasourceStatus;
import io.mycat.replica.heartbeat.HeartBeatStatus;
import io.mycat.replica.heartbeat.HeartbeatManager;
import io.mycat.replica.heartbeat.NoneHeartbeatDetector;
import io.mycat.replica.heartbeat.strategy.MySQLMasterSlaveBeatStrategy;
import io.mycat.replica.heartbeat.strategy.MySQLSingleHeartBeatStrategy;
import java.util.Objects;

/**
 * @author : zhangwy
 * @version V1.0 date Date : 2019年05月14日 22:21
 */
public class MySQLProxyHeartBeatManager extends HeartbeatManager {

  final static MycatLogger LOGGER = MycatLoggerFactory.getLogger(MySQLProxyHeartBeatManager.class);
  private final MySQLDatasource dataSource;

  public MySQLProxyHeartBeatManager(ProxyRuntime runtime, ReplicaConfig replicaConfig,
      MySQLDataSourceEx dataSource) {
    HeartbeatRootConfig heartbeatRootConfig = runtime.getConfig(ConfigEnum.HEARTBEAT);
    ////////////////////////////////////check/////////////////////////////////////////////////
    Objects.requireNonNull(heartbeatRootConfig, "heartbeat config can not found");
    Objects
        .requireNonNull(heartbeatRootConfig.getHeartbeat(), "heartbeat config can not be empty");
    ////////////////////////////////////check/////////////////////////////////////////////////
    this.dataSource = dataSource;
    this.dsStatus = new DatasourceStatus();
    long lastSwitchTime = System.currentTimeMillis();
    HeartbeatConfig heartbeatConfig = heartbeatRootConfig
        .getHeartbeat();
    int maxRetry = heartbeatConfig.getMaxRetry();
    long minSwitchTimeInterval = heartbeatConfig.getMinSwitchTimeInterval();
    this.hbStatus = new HeartBeatStatus(maxRetry, minSwitchTimeInterval, false, lastSwitchTime);

    if (ReplicaConfig.RepTypeEnum.SINGLE_NODE.equals(replicaConfig.getRepType())) {
      this.heartbeatDetector = new DefaultProxyHeartbeatDetector(runtime, replicaConfig, dataSource,
          this,
          MySQLSingleHeartBeatStrategy::new);
    } else if (ReplicaConfig.RepTypeEnum.MASTER_SLAVE.equals(replicaConfig.getRepType())) {
      this.heartbeatDetector = new DefaultProxyHeartbeatDetector(runtime, replicaConfig, dataSource,
          this,
          MySQLMasterSlaveBeatStrategy::new);
    } else if (ReplicaConfig.RepTypeEnum.GARELA_CLUSTER.equals(replicaConfig.getRepType())) {
      this.heartbeatDetector = new DefaultProxyHeartbeatDetector(runtime, replicaConfig, dataSource,
          this,
          MySQLSingleHeartBeatStrategy::new);
    } else {
      this.heartbeatDetector = new NoneHeartbeatDetector();
    }
  }


  //给所有的mycatThread发送dataSourceStatus
  @Override
  public void sendDataSourceStatus(DatasourceStatus currentDatasourceStatus) {
    //状态不同进行状态的同步
    if (!this.dsStatus.equals(currentDatasourceStatus)) {
      //设置状态给 dataSource
      this.dsStatus = currentDatasourceStatus;
      LOGGER.error("{} heartStatus {}", dataSource.getName(), dsStatus);
    }
    ReplicaConfig conf = this.dataSource.getReplica().getConfig();
    if (conf.getSwitchType().equals(RepSwitchTypeEnum.SWITCH)
        && dataSource.isMaster() && dsStatus.isError()
        && canSwitchDataSource()) {
      //replicat 进行选主
      if (dataSource.getReplica().switchDataSourceIfNeed()) {
        //updataSwitchTime
        this.hbStatus.setLastSwitchTime(System.currentTimeMillis());
      }
    }
  }

}
