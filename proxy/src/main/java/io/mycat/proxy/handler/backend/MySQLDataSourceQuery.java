package io.mycat.proxy.handler.backend;

import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.proxy.session.SessionManager.SessionIdAble;

import java.util.List;

public class MySQLDataSourceQuery {

  boolean runOnMaster = true;
  LoadBalanceStrategy strategy = null;
  List<SessionIdAble> ids = null;
  public boolean isRunOnMaster() {
    return runOnMaster;
  }

  public void setRunOnMaster(boolean runOnMaster) {
    this.runOnMaster = runOnMaster;
  }

  public LoadBalanceStrategy getStrategy() {
    return strategy;
  }

  public void setStrategy(LoadBalanceStrategy strategy) {
    this.strategy = strategy;
  }

  public List<SessionIdAble> getIds() {
    return ids;
  }

  public void setIds(List<SessionIdAble> ids) {
    this.ids = ids;
  }

}