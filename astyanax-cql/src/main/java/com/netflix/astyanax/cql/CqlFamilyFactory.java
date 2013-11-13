package com.netflix.astyanax.cql;

import java.util.concurrent.atomic.AtomicBoolean;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.MetricsOptions;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.Policies;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.netflix.astyanax.AstyanaxConfiguration;
import com.netflix.astyanax.AstyanaxTypeFactory;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.KeyspaceTracerFactory;
import com.netflix.astyanax.connectionpool.Connection;
import com.netflix.astyanax.connectionpool.ConnectionFactory;
import com.netflix.astyanax.connectionpool.ConnectionPool;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.ConnectionPoolProxy;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.exceptions.ThrottledException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;

public class CqlFamilyFactory implements AstyanaxTypeFactory<Cluster> {

	private static CqlFamilyFactory Instance = new CqlFamilyFactory(); 
	
	private static AtomicBoolean BatchColumnUpdates = new AtomicBoolean(false);
	
	public static CqlFamilyFactory getInstance() {
		return Instance;
	}
	
	@Override
	public Keyspace createKeyspace(String ksName, ConnectionPool<Cluster> cp, AstyanaxConfiguration asConfig, KeyspaceTracerFactory tracerFactory) {
		
		if (!(cp instanceof ConnectionPoolProxy)) {
			throw new RuntimeException("Cannot use CqlFamilyFactory with a connection pool type other than ConnectionPoolType.JAVA_DRIVER");
		}

		ConnectionPoolProxy<?> cpProxy = (ConnectionPoolProxy<?>)cp; 
		
		CqlKeyspaceImpl keyspace = new CqlKeyspaceImpl(ksName, asConfig, tracerFactory, cpProxy.getConnectionPoolConfiguration());
		cpProxy.addListener(keyspace);
		
		return keyspace;
	}

	@Override
	public com.netflix.astyanax.Cluster createCluster(ConnectionPool<Cluster> cp, AstyanaxConfiguration asConfig, KeyspaceTracerFactory tracerFactory) {
		
		if (!(cp instanceof ConnectionPoolProxy)) {
			throw new RuntimeException("Cannot use CqlFamilyFactory with a connection pool type other than ConnectionPoolType.JAVA_DRIVER");
		}
		
		ConnectionPoolProxy<?> cpProxy = (ConnectionPoolProxy<?>)cp; 
		CqlClusterImpl cluster = new CqlClusterImpl(asConfig, tracerFactory, cpProxy.getConnectionPoolConfiguration());
		((ConnectionPoolProxy<Cluster>)cp).addListener(cluster);
		
		return cluster;
	}

	@Override
	public ConnectionFactory<Cluster> createConnectionFactory(
			AstyanaxConfiguration asConfig,
			ConnectionPoolConfiguration cfConfig,
			KeyspaceTracerFactory tracerFactory, 
			ConnectionPoolMonitor monitor) {
		
		CqlBasedConnectionFactory<Cluster> factory = new CqlBasedConnectionFactory<Cluster>();
		factory.asConfig = asConfig;
		factory.cfConfig = cfConfig;
		factory.tracerFactory = tracerFactory;
		factory.monitor = monitor;
		
		return factory;
	}
	
	@SuppressWarnings("unused")
	private static class CqlBasedConnectionFactory<T> implements ConnectionFactory<T> {
		
		protected AstyanaxConfiguration asConfig;
		protected ConnectionPoolConfiguration cfConfig;
		protected KeyspaceTracerFactory tracerFactory;
		protected ConnectionPoolMonitor monitor;
		
		@Override
		public Connection<T> createConnection(HostConnectionPool<T> pool) throws ThrottledException {
			throw new NotImplementedException();
		}
	}	
	
	public CqlFamilyFactory enableColumnBatchUpdates(boolean condition) {
		BatchColumnUpdates.set(condition);
		return this;
	}
	
	public static boolean batchColumnUpdates() {
		return BatchColumnUpdates.get();
	}
	
	
	private Configuration getOrCreateJDConfiguration(AstyanaxConfiguration asConfig, ConnectionPoolConfiguration cpConfig) {
		
		if (asConfig.getConnectionPoolType() == ConnectionPoolType.BAG) {
		}

		Configuration actualConfig = null; 
		
		JavaDriverConnectionPoolConfigurationImpl jdConfig = (JavaDriverConnectionPoolConfigurationImpl) cpConfig;
		if (jdConfig != null) {
			if (jdConfig.getJavaDriverConfig() != null) {
				actualConfig = jdConfig.getJavaDriverConfig();
				return actualConfig;
			}
		}
		
		LoadBalancingPolicy lbPolicy = null;
		switch (asConfig.getConnectionPoolType()) {
		case BAG:
				throw new RuntimeException("Cannot use ConnectionPoolType.BAG with java driver, " +
					"use TOKEN_AWARE or ROUND_ROBIN or configure java driver directly");
		case ROUND_ROBIN:
				lbPolicy = new RoundRobinPolicy();
				break;
		case TOKEN_AWARE:
				lbPolicy = new TokenAwarePolicy(new RoundRobinPolicy());
				break;
		};
		
		Policies policies = new Policies(lbPolicy, Policies.defaultReconnectionPolicy(), Policies.defaultRetryPolicy());
		
		return new Configuration(
				policies,
				new ProtocolOptions(),
				new PoolingOptions(),
				new SocketOptions(),
				new MetricsOptions(),
				new QueryOptions());
	}
}
