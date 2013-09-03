package com.netflix.astyanax.cql;

import java.util.concurrent.atomic.AtomicBoolean;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.datastax.driver.core.Cluster;
import com.netflix.astyanax.AstyanaxConfiguration;
import com.netflix.astyanax.AstyanaxTypeFactory;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.KeyspaceTracerFactory;
import com.netflix.astyanax.connectionpool.Connection;
import com.netflix.astyanax.connectionpool.ConnectionFactory;
import com.netflix.astyanax.connectionpool.ConnectionPool;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.CqlConnectionPoolProxy;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.exceptions.ThrottledException;

public class CqlFamilyFactory implements AstyanaxTypeFactory<Cluster> {

	private static CqlFamilyFactory Instance = new CqlFamilyFactory(); 
	
	private static AtomicBoolean NewCqlMode = new AtomicBoolean(false);
	private static AtomicBoolean BatchColumnUpdates = new AtomicBoolean(false);
	
	public static CqlFamilyFactory getInstance() {
		return Instance;
	}
	
	@Override
	public Keyspace createKeyspace(String ksName, ConnectionPool<Cluster> cp, AstyanaxConfiguration asConfig, KeyspaceTracerFactory tracerFactory) {
		
		if (!(cp instanceof CqlConnectionPoolProxy)) {
			throw new RuntimeException("Cannot use CqlFamilyFactory with a connection pool type other than ConnectionPoolType.JAVA_DRIVER");
		}

		CqlKeyspaceImpl keyspace = new CqlKeyspaceImpl(ksName, asConfig, tracerFactory);
		
		((CqlConnectionPoolProxy<Cluster>)cp).addListener(keyspace);
		
		return keyspace;
	}

	@Override
	public com.netflix.astyanax.Cluster createCluster(ConnectionPool<Cluster> cp, AstyanaxConfiguration asConfig, KeyspaceTracerFactory tracerFactory) {
		
		if (!(cp instanceof CqlConnectionPoolProxy)) {
			throw new RuntimeException("Cannot use CqlFamilyFactory with a connection pool type other than ConnectionPoolType.JAVA_DRIVER");
		}
		
		CqlClusterImpl cluster = new CqlClusterImpl(asConfig, tracerFactory);
		((CqlConnectionPoolProxy<Cluster>)cp).addListener(cluster);
		
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
	
	public CqlFamilyFactory enableNewCqlMode(boolean condition) {
		NewCqlMode.set(condition);
		return this;
	}
	
	public static boolean OldStyleThriftMode() {
		return !NewCqlMode.get();
	}

	public static boolean NewCqlMode() {
		return NewCqlMode.get();
	}

	public CqlFamilyFactory enableColumnBatchUpdates(boolean condition) {
		BatchColumnUpdates.set(condition);
		return this;
	}
	
	public static boolean batchColumnUpdates() {
		return BatchColumnUpdates.get();
	}
}
