package io.mycat;

import io.mycat.config.MycatServerConfig;
import io.mycat.config.ServerConfiguration;
import io.mycat.config.ServerConfigurationImpl;
import io.mycat.plug.loadBalance.LoadBalanceManager;
import io.mycat.proxy.session.ProxyAuthenticator;
import lombok.SneakyThrows;
import org.apache.calcite.mycat.MycatBuiltInMethod;
import org.apache.calcite.util.BuiltInMethod;
import sun.util.calendar.ZoneInfo;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Optional;
import java.util.TimeZone;

/**
 * @author cjw
 **/
public class MycatCore {
    public static final String PROPERTY_MODE_LOCAL = "local";
    public static final String PROPERTY_MODE_CLUSTER = "cluster";
    public static final String PROPERTY_METADATADIR = "metadata";
    private final MycatServer mycatServer;
    private final MetadataStorageManager metadataStorageManager;
    private final Path baseDirectory;

    public MycatCore() {
        this(null);
    }

    @SneakyThrows
    public MycatCore(String path) {
        MycatBuiltInMethod booleanToBigint = MycatBuiltInMethod.BOOLEAN_TO_BIGINT;
        // TimeZone.setDefault(ZoneInfo.getTimeZone("UTC"));
        if (path == null) {
            String configResourceKeyName = "MYCAT_HOME";
            path = System.getProperty(configResourceKeyName);
        }
        if (path == null) {
            path =  Paths.get(this.getClass().getProtectionDomain().getCodeSource().getLocation().toURI()).toString();;
        }
        this.baseDirectory = Paths.get(path).getParent().getParent().toAbsolutePath();
        System.out.println("path:" + this.baseDirectory);
        ServerConfiguration serverConfiguration = new ServerConfigurationImpl(MycatCore.class, path);
        MycatServerConfig serverConfig = serverConfiguration.serverConfig();
        String datasourceProvider = serverConfig.getDatasourceProvider();
        this.mycatServer = new MycatServer(serverConfig, new ProxyAuthenticator(), new ProxyDatasourceConfigProvider());
        LoadBalanceManager loadBalanceManager = mycatServer.getLoadBalanceManager();
        MycatWorkerProcessor mycatWorkerProcessor = mycatServer.getMycatWorkerProcessor();

        HashMap<Class, Object> context = new HashMap<>();
        context.put(serverConfiguration.getClass(), serverConfiguration);
        context.put(serverConfig.getClass(), serverConfig);
        context.put(loadBalanceManager.getClass(), loadBalanceManager);
        context.put(mycatWorkerProcessor.getClass(), mycatWorkerProcessor);
        context.put(mycatServer.getClass(), mycatServer);
        MetaClusterCurrent.register(context);

        String mode = Optional.ofNullable(serverConfig.getMode()).orElse(PROPERTY_MODE_LOCAL).toLowerCase();
        switch (mode) {
            case PROPERTY_MODE_LOCAL: {
                metadataStorageManager = new FileMetadataStorageManager(serverConfig,datasourceProvider, this.baseDirectory);
                break;
            }
            case PROPERTY_MODE_CLUSTER:
                String zkAddress = System.getProperty("zkAddress");
                if (zkAddress != null) {
                    ZKStore zkStore = new ZKStore("mycat", zkAddress);
                    metadataStorageManager =
                            new CoordinatorMetadataStorageManager(serverConfig,zkStore,
                                    ConfigReaderWriter.getReaderWriterBySuffix("json"),
                                    datasourceProvider);
                    break;
                }
            default: {
                throw new UnsupportedOperationException();
            }
        }

        context.put(metadataStorageManager.getClass(), metadataStorageManager);
        MetaClusterCurrent.register(context);
    }

    public void start() throws Exception {
        metadataStorageManager.start();
        mycatServer.start();
    }

    public static void main(String[] args)throws Exception  {
        new MycatCore().start();
    }
}
