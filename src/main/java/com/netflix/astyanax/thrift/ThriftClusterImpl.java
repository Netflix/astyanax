package com.netflix.astyanax.thrift;


import java.util.concurrent.Future;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.CfDef;
import org.apache.thrift.TException;

import com.netflix.astyanax.AstyanaxConfiguration;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.connectionpool.ConnectionPool;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.KeyspaceDefinition;

public class ThriftClusterImpl implements Cluster {

	private final ConnectionPool<Cassandra.Client> connectionPool;
	
	public ThriftClusterImpl(AstyanaxConfiguration config) {
		this.connectionPool = config
			.getConnectionPoolFactory()
				.createConnectionPool(config, 
					new ThriftSyncConnectionFactoryImpl(config));
	}
	
	@Override
	public String describeClusterName() throws ConnectionException, OperationException {
		return connectionPool.executeWithFailover(new AbstractClusterOperationImpl<String>() {
			@Override
			public String execute(Client client) throws ConnectionException {
				try {
					return client.describe_cluster_name();
				} catch (TException e) {
					throw ThriftConverter.ToConnectionPoolException(e);
				}
			}

		}).getResult();
	}

	@Override
	public String describeSnitch() throws ConnectionException, OperationException {
		return connectionPool.executeWithFailover(new AbstractClusterOperationImpl<String>() {
			@Override
			public String execute(Client client) throws ConnectionException {
				try {
					return client.describe_snitch();
				} catch (TException e) {
					throw ThriftConverter.ToConnectionPoolException(e);
				}
			}
		}).getResult();
	}

	@Override
	public String describePartitioner() throws ConnectionException, OperationException {
		return connectionPool.executeWithFailover(new AbstractClusterOperationImpl<String>() {
			@Override
			public String execute(Client client) throws ConnectionException {
				try {
					return client.describe_partitioner();
				} catch (TException e) {
					throw ThriftConverter.ToConnectionPoolException(e);
				}
			}
		}).getResult();
	}

	/**
	 * Get the version from the cluster
	 * @return
	 * @throws OperationException 
	 */
	@Override
	public String getVersion() throws ConnectionException, OperationException {
		return connectionPool.executeWithFailover(new AbstractClusterOperationImpl<String>() {
			@Override
			public String execute(Client client) throws ConnectionException {
				try {
					return client.describe_version();
				} catch (TException e) {
					throw ThriftConverter.ToConnectionPoolException(e);
				}
			}
		}).getResult();
	}
	
	@Override
	public String dropColumnFamily(final String keyspaceName, final String columnFamilyName) throws OperationException, ConnectionException {
		return connectionPool.executeWithFailover(new AbstractClusterOperationImpl<String>() {
			@Override
			public String execute(Client client) throws ConnectionException, OperationException {
				try {
					client.set_keyspace(keyspaceName);
					return client.system_drop_column_family(columnFamilyName);
				} catch (Exception e) {
					throw ThriftConverter.ToConnectionPoolException(e);
				}
			}
		}).getResult();
	}
	
	@Override
	public String dropKeyspace(final String keyspaceName) throws OperationException, ConnectionException {
		return connectionPool.executeWithFailover(new AbstractClusterOperationImpl<String>() {
			@Override
			public String execute(Client client) throws ConnectionException, OperationException {
				try {
					return client.system_drop_keyspace(keyspaceName);
				} catch (Exception e) {
					throw ThriftConverter.ToConnectionPoolException(e);
				}
			}
		}).getResult();
	}
	
	@Override
	public ColumnFamilyDefinition prepareColumnFamilyDefinition() {
		final CfDef cfDef = new CfDef();
		cfDef.setColumn_type("Standard");
		
		return new AbstractColumnFamilyDefinitionImpl(cfDef, null) {

			@Override
			public OperationResult<String> execute() throws ConnectionException {
				return connectionPool.executeWithFailover(new AbstractClusterOperationImpl<String>() {
					@Override
					public String execute(Client client) throws ConnectionException, OperationException {
						try {
							return client.system_add_column_family(cfDef);
						} catch (Exception e) {
							throw ThriftConverter.ToConnectionPoolException(e);
						}
					}
				});
			}
			
			@Override
			public KeyspaceDefinition endColumnFamilyDefinition() {
				throw new IllegalStateException();
			}

			@Override
			public Future<OperationResult<String>> executeAsync() throws ConnectionException {
				throw new UnsupportedOperationException();
			}
		};
	}
	
	@Override
	public KeyspaceDefinition prepareKeyspaceDefinition() {
		return new AbstractKeyspaceDefinitionImpl() {
			@Override
			public OperationResult<String> execute() throws ConnectionException {
				return connectionPool.executeWithFailover(new AbstractClusterOperationImpl<String>() {
					@Override
					public String execute(Client client) throws ConnectionException, OperationException {
						try {
							return client.system_add_keyspace(ks_def);
						} catch (Exception e) {
							throw ThriftConverter.ToConnectionPoolException(e);
						}
					}
				});
			}

			@Override
			public Future<OperationResult<String>> executeAsync()
					throws ConnectionException {
				// TODO Auto-generated method stub
				return null;
			}
		};
	}
}
