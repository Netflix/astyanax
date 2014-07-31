package com.netflix.astyanax.cql;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.NotImplementedException;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Configuration;
import com.netflix.astyanax.AstyanaxConfiguration;
import com.netflix.astyanax.AstyanaxContext;
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
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;

/**
 * Simple impl of {@link AstyanaxTypeFactory} that acts as the bridge between the AstyanaxContext setup and all the java driver setup.
 * The main link is the {@link ConnectionPoolProxy} class which gives us access to the {@link ConnectionPoolConfiguration} object. 
 * The class expects a {@link JavaDriverConnectionPoolConfigurationImpl} based impl which encapsulates all the config that is required
 * by java driver. 
 * 
 * Thus this bridge is built with the intention to let the outside caller to directly use the {@link Configuration} object and inject it 
 * using {@link AstyanaxContext}. 
 * 
 * Restating, the simple flow that enables the bridge is
 * 1. Construct the {@link Configuration} object with all the desired options for configuring the java driver.
 * 2. Construct the {@link JavaDriverConnectionPoolConfigurationImpl} object and pass the java driver configuration object to it. 
 * 3. Set the {@link ConnectionPoolConfiguration} created in step 2. on the {@link AstyanaxContext} builder object when creating the Astyanax {@link Keyspace}  
 * 
 * See {@link AstyanaxContext} for more details on how to do this. 
 * 
 * @author poberai
 *
 */
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
		
		ConnectionPoolConfiguration jdConfig = getOrCreateJDConfiguration(asConfig, cpProxy.getConnectionPoolConfiguration());
		ConnectionPoolMonitor monitor = cpProxy.getConnectionPoolMonitor();
		if (monitor != null && (monitor instanceof CountingConnectionPoolMonitor))
			monitor = new JavaDriverConnectionPoolMonitorImpl();
		CqlKeyspaceImpl keyspace = new CqlKeyspaceImpl(ksName, asConfig, tracerFactory, jdConfig, monitor);
		cpProxy.addListener(keyspace);
		
		return keyspace;
	}

	@Override
	public com.netflix.astyanax.Cluster createCluster(ConnectionPool<Cluster> cp, AstyanaxConfiguration asConfig, KeyspaceTracerFactory tracerFactory) {
		
		if (!(cp instanceof ConnectionPoolProxy)) {
			throw new RuntimeException("Cannot use CqlFamilyFactory with a connection pool type other than ConnectionPoolType.JAVA_DRIVER");
		}
		
		ConnectionPoolProxy<?> cpProxy = (ConnectionPoolProxy<?>)cp; 
		ConnectionPoolConfiguration jdConfig = getOrCreateJDConfiguration(asConfig, cpProxy.getConnectionPoolConfiguration());
		ConnectionPoolMonitor monitor = cpProxy.getConnectionPoolMonitor();
		if (monitor != null && (monitor instanceof CountingConnectionPoolMonitor))
			monitor = new JavaDriverConnectionPoolMonitorImpl();
		CqlClusterImpl cluster = new CqlClusterImpl(asConfig, tracerFactory, jdConfig, monitor);
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
	
	
	private ConnectionPoolConfiguration getOrCreateJDConfiguration(AstyanaxConfiguration asConfig, ConnectionPoolConfiguration cpConfig) {
		
		if (cpConfig instanceof JavaDriverConnectionPoolConfigurationImpl) {
			JavaDriverConnectionPoolConfigurationImpl jdConfig = (JavaDriverConnectionPoolConfigurationImpl) cpConfig;
			if (jdConfig.getJavaDriverConfig() != null) {
				return jdConfig;  // Java Driver config has already been setup.
			}
		}
		
		// Else create Java Driver Config from AstyanaxConfiguration and ConnectionPoolConfiguration and return that. 
		return new JavaDriverConnectionPoolConfigurationImpl(new JavaDriverConfigBridge(asConfig, cpConfig).getJDConfig());
	}
}
