package com.netflix.astyanax.thrift;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.thrift.TException;

import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.connectionpool.ConnectionPool;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;

public class ThriftClusterImpl implements Cluster {

	private final ConnectionPool<Cassandra.Client> pool;
	
	public ThriftClusterImpl(ConnectionPool<Cassandra.Client> pool) {
		this.pool = pool;
	}
	
	@Override
	public String describeClusterName() throws ConnectionException, OperationException {
		return pool.executeWithFailover(new AbstractClusterOperationImpl<String>() {
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
		return pool.executeWithFailover(new AbstractClusterOperationImpl<String>() {
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
		return pool.executeWithFailover(new AbstractClusterOperationImpl<String>() {
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
		return pool.executeWithFailover(new AbstractClusterOperationImpl<String>() {
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
}
