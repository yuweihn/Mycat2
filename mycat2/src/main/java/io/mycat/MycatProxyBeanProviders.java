package io.mycat;

import io.mycat.beans.mycat.MySQLDataNode;
import io.mycat.command.CommandDispatcher;
import io.mycat.config.ConfigFile;
import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.config.proxy.ProxyRootConfig;
import io.mycat.config.schema.DataNodeConfig;
import io.mycat.datasource.jdbc.GRuntime;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.handler.backend.MySQLSynContext;
import io.mycat.proxy.handler.backend.MySQLSynContextImpl;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MycatSession;
import io.mycat.replica.MySQLDatasource;
import io.mycat.replica.MySQLReplica;
import io.mycat.router.MycatRouterConfig;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * @author jamie12221 date 2019-05-22 22:12
 **/
public class MycatProxyBeanProviders implements ProxyBeanProviders {

  @Override
  public void initRuntime(ProxyRuntime runtime, Map<String, Object> defContext) throws Exception {

  }

  @Override
  public void beforeAcceptConnectionProcess(ProxyRuntime runtime, Map<String, Object> defContext)
      throws Exception {
    defContext.put("routerConfig",
        new MycatRouterConfig(runtime.getConfig(), runtime.getMySQLAPIRuntime()));
    GRuntime.INSTACNE.load(ConfigRuntime.INSTCANE.load());
    GRuntime.INSTACNE.getDefContext().putAll(defContext);
  }

  @Override
  public MySQLDatasource createDatasource(ProxyRuntime runtime, int index,
      DatasourceConfig datasourceConfig,
      MySQLReplica replica) {
    return new MySQLDatasource(index, datasourceConfig, replica) {
    };
  }

  @Override
  public MySQLReplica createReplica(ProxyRuntime runtime, ReplicaConfig replicaConfig,
      Set<Integer> writeIndex) {
    return new MySQLReplica(runtime, replicaConfig, this) {
    };
  }

  @Override
  public MySQLDataNode createMySQLDataNode(ProxyRuntime runtime, DataNodeConfig config) {
    return new MySQLDataNode(config);
  }

  @Override
  public CommandDispatcher createCommandDispatcher(ProxyRuntime runtime, MycatSession session) {
    ProxyRootConfig config = runtime.getConfig(ConfigFile.PROXY);
    Objects.requireNonNull(config);
    String commandDispatcherClass = config.getProxy().getCommandDispatcherClass();
    CommandDispatcher commandDispatcher;
    Class<?> clz = null;
    try {
      clz = Class.forName(commandDispatcherClass);
      commandDispatcher = (CommandDispatcher) clz.newInstance();
      commandDispatcher.initRuntime(session, runtime);
      return commandDispatcher;
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public MySQLSynContext createMySQLSynContext(MycatSession mycat) {
    return new MySQLSynContextImpl(mycat);
  }

  @Override
  public MySQLSynContext createMySQLSynContext(MySQLClientSession mysql) {
    return new MySQLSynContextImpl(mysql);
  }


}
