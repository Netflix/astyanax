package com.netflix.astyanax.cql.test.utils;

import static com.netflix.astyanax.cql.test.utils.ClusterConfiguration.TEST_CLUSTER_NAME;
import static com.netflix.astyanax.cql.test.utils.ClusterConfiguration.TEST_KEYSPACE_NAME;
import static com.netflix.astyanax.cql.test.utils.ClusterConfiguration.TheDriver;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.PropertyConfigurator;

import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.MetricsOptions;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.Policies;
import com.google.common.base.Supplier;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.cql.CqlFamilyFactory;
import com.netflix.astyanax.cql.JavaDriverConnectionPoolConfigurationImpl;
import com.netflix.astyanax.cql.test.utils.ClusterConfiguration.Driver;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public class AstyanaxContextFactory {

    private static final AtomicReference<Keyspace> keyspaceReference = new AtomicReference<Keyspace>(null);
    
    static {
    	PropertyConfigurator.configure("./src/main/java/test-log4j.properties");

    	AstyanaxContext<Keyspace> context = AstyanaxContextFactory.getKeyspace();
    	context.start();
    	keyspaceReference.set(context.getClient());
    }
    
    public static AstyanaxContext<Cluster> getCluster() {
    	return getCluster(TEST_CLUSTER_NAME, TheDriver);
    }
    
    public static AstyanaxContext<Cluster> getCluster(String clusterName, Driver driver) {
    	if (driver == Driver.JAVA_DRIVER) {
    		return clusterWithJavaDriver(clusterName);
    	} else {
    		return clusterWithThriftDriver(clusterName);
    	}
    }

    private static AstyanaxContext<Cluster> clusterWithJavaDriver(String clusterName) {

    	final String SEEDS = "localhost";

		Supplier<List<Host>> HostSupplier = new Supplier<List<Host>>() {

			@Override
			public List<Host> get() {
				Host host = new Host(SEEDS, -1);
				return Collections.singletonList(host);
			}
    	};
    	
    	AstyanaxContext<Cluster> context = new AstyanaxContext.Builder()
                .forCluster(clusterName)
                .withAstyanaxConfiguration(
                        new AstyanaxConfigurationImpl()
                                .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                                .setDiscoveryDelayInSeconds(60000))
                .withConnectionPoolConfiguration(
                        new ConnectionPoolConfigurationImpl(TEST_CLUSTER_NAME
                                + "_" + TEST_KEYSPACE_NAME)
                                .setSocketTimeout(30000)
                                .setMaxTimeoutWhenExhausted(2000)
                                .setMaxConnsPerHost(20)
                                .setInitConnsPerHost(10)
                                .setSeeds(SEEDS)
                                .setPort(9042)
                                )
                .withHostSupplier(HostSupplier)
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildCluster(CqlFamilyFactory.getInstance());

    	return context;
    }
    
    private static AstyanaxContext<Cluster> clusterWithThriftDriver(String clusterName) {
    	final String SEEDS = "localhost";

		Supplier<List<Host>> HostSupplier = new Supplier<List<Host>>() {

			@Override
			public List<Host> get() {
				Host host = new Host(SEEDS, -1);
				return Collections.singletonList(host);
			}
    	};
    	
    	AstyanaxContext<Cluster> context = new AstyanaxContext.Builder()
    	.forCluster(clusterName)
    	.withAstyanaxConfiguration(
    			new AstyanaxConfigurationImpl()
    			.setDiscoveryType(NodeDiscoveryType.DISCOVERY_SERVICE)
    			.setConnectionPoolType(ConnectionPoolType.ROUND_ROBIN)
    			.setDiscoveryDelayInSeconds(60000))
    			.withConnectionPoolConfiguration(
    					new ConnectionPoolConfigurationImpl(TEST_CLUSTER_NAME
    							+ "_" + TEST_KEYSPACE_NAME)
    					.setSocketTimeout(30000)
    					.setMaxTimeoutWhenExhausted(2000)
    					.setMaxConnsPerHost(20)
    					.setInitConnsPerHost(10)
    					.setSeeds(SEEDS)
    					.setPort(9160)
    					)
    					.withHostSupplier(HostSupplier)
    					.withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
    					.buildCluster(ThriftFamilyFactory.getInstance());

    	return context;
    }

    public static AstyanaxContext<Keyspace> getKeyspace() {
    	return getKeyspace(TEST_KEYSPACE_NAME, TheDriver); 
    }

    public static AstyanaxContext<Keyspace> getKeyspace(String keyspaceName) {
    	return getKeyspace(keyspaceName, TheDriver); 
    }

    public static AstyanaxContext<Keyspace> getKeyspace(String keyspaceName, Driver driver) {
    	if (driver == Driver.THRIFT) {
        	return keyspaceWithThriftDriver(keyspaceName); 
    	} else {
    		return keyspaceWithJavaDriver(keyspaceName);
    	}
    }
    
    private static AstyanaxContext<Keyspace> keyspaceWithJavaDriver(String keyspaceName) {

    	final String SEEDS = "localhost";

		Supplier<List<Host>> HostSupplier = new Supplier<List<Host>>() {

			@Override
			public List<Host> get() {
				Host host = new Host(SEEDS, -1);
				return Collections.singletonList(host);
			}
    	};
    	
    	ProtocolOptions protocolOptions = new ProtocolOptions(9042);
		
		Configuration jdConfig = new Configuration(new Policies(),
	             protocolOptions,
	             new PoolingOptions(),
	             new SocketOptions(),
	             new MetricsOptions(),
	             new QueryOptions());

		AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
		.forKeyspace(keyspaceName)
		.withHostSupplier(HostSupplier)
		.withAstyanaxConfiguration(new AstyanaxConfigurationImpl())
		.withConnectionPoolConfiguration(new JavaDriverConnectionPoolConfigurationImpl(jdConfig))
		.buildKeyspace(CqlFamilyFactory.getInstance());

    	return context;
    }
    
    private static AstyanaxContext<Keyspace> keyspaceWithThriftDriver(String keyspaceName) {

    	final String SEEDS = "localhost";

		Supplier<List<Host>> HostSupplier = new Supplier<List<Host>>() {

			@Override
			public List<Host> get() {
				Host host = new Host(SEEDS, 9160);
				return Collections.singletonList(host);
			}
    	};
    	
    	AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
                .forCluster(TEST_CLUSTER_NAME)
                .forKeyspace(keyspaceName)
                .withAstyanaxConfiguration(
                        new AstyanaxConfigurationImpl()
                                .setDiscoveryType(NodeDiscoveryType.DISCOVERY_SERVICE)
                                .setConnectionPoolType(ConnectionPoolType.ROUND_ROBIN)
                                .setDiscoveryDelayInSeconds(60000)
                                .setTargetCassandraVersion("1.2")
                                )
                                .withConnectionPoolConfiguration(
                        new ConnectionPoolConfigurationImpl(TEST_CLUSTER_NAME
                                + "_" + TEST_KEYSPACE_NAME)
                                .setSocketTimeout(30000)
                                .setMaxTimeoutWhenExhausted(2000)
                                .setMaxConnsPerHost(20)
                                .setInitConnsPerHost(10)
                                .setSeeds(SEEDS)
                                .setPort(9160)
                                )
                .withHostSupplier(HostSupplier)
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildKeyspace(ThriftFamilyFactory.getInstance());

    	return context;
    }

    public static Keyspace getCachedKeyspace() {
    	return keyspaceReference.get();
    }
}
