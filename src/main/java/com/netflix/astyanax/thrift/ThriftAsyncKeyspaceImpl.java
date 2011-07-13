package com.netflix.astyanax.thrift;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Cassandra.AsyncClient.describe_ring_call;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.netflix.astyanax.CounterMutation;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.KeyspaceTracers;
import com.netflix.astyanax.Query;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.ConnectionPool;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.NodeDiscovery;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.connectionpool.impl.TokenRangeImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ColumnPath;
import com.netflix.astyanax.model.KeySlice;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.model.TokenRange;
import com.netflix.astyanax.query.ColumnFamilyQuery;

public class ThriftAsyncKeyspaceImpl implements Keyspace {

	private final ConnectionPool<Cassandra.AsyncClient> connectionPool;
	private final RandomPartitioner partitioner;
	private final NodeDiscovery discovery;
	private final ConnectionPoolConfiguration config;
	private final KeyspaceTracers tracers;

	public ThriftAsyncKeyspaceImpl(ConnectionPoolConfiguration config) throws IOException {
		this.config = config;
		this.connectionPool = config
			.getConnectionPoolFactory()
				.createConnectionPool(config, 
					new ThriftAsyncConnectionFactoryImpl(config.getKeyspaceName()));
		this.partitioner = new RandomPartitioner();
		this.discovery = config
			.getNodeDiscoveryFactory().
				createNodeDiscovery(config, this, connectionPool);
		if (this.discovery != null) {
			this.discovery.start();
		}
		this.tracers = config.getKeyspaceTracers();
	}
	
	@Override
	public String getKeyspaceName() {
		return this.config.getKeyspaceName();
	}
	
	static class Result<R> {
		private R result;
		
		public void setResult(R result) {
			this.result = result;
		}
		
		public R getResult() {
			return this.result;
		}
	}

	@Override
	public List<TokenRange> describeRing() throws ConnectionException, OperationException {
		OperationResult<List<TokenRange>> result = 
			connectionPool.executeWithFailover(new Operation<Cassandra.AsyncClient, List<TokenRange>>() {
			@Override
			public List<TokenRange> execute(Cassandra.AsyncClient client) throws ConnectionException {
				try {
					final CountDownLatch latch = new CountDownLatch(1);
					final Result<List<TokenRange>> tokens = new Result<List<TokenRange>>();
					client.describe_ring(config.getKeyspaceName(), new AsyncMethodCallback<describe_ring_call>() {

						@Override
						public void onComplete(describe_ring_call response) {
							try {
								tokens.setResult(Lists.transform(response.getResult(), 
										new Function<org.apache.cassandra.thrift.TokenRange, TokenRange>() {
									@Override
									public TokenRange apply(
											org.apache.cassandra.thrift.TokenRange tr) {
										return new TokenRangeImpl(tr.getStart_token(), tr.getEnd_token(), tr.getEndpoints());
									}
									
								}));
							} catch (InvalidRequestException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} catch (TException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							finally {
								latch.countDown();
							}
						}

						@Override
						public void onError(Exception exception) {
							// TODO Auto-generated method stub
							
						}
					
					});
					return tokens.getResult();
				} 
				catch (Exception e) {
					throw ThriftConverter.ToConnectionPoolException(e);
				}
			}

			@Override
			public BigInteger getKey() {
				return null;
			}

			@Override
			public String getKeyspace() {
				// TODO Auto-generated method stub
				return null;
			}
		});
		
		return result.getResult();
	}

	@Override
	public MutationBatch prepareMutationBatch() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, C> Query<K, C, ColumnList<C>> prepareGetRowQuery(
			ColumnFamily<K, ?> columnFamily, Serializer<C> columnSerializer, K rowKey) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, C> Query<K, C, Rows<K, C>> prepareGetMultiRowQuery(
			ColumnFamily<K, ?> columnFamily, Serializer<C> columnSerializer, KeySlice<K> keys) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, C> Query<K, C, Column<C>> prepareGetColumnQuery(
			ColumnFamily<K, ?> columnFamily, K rowKey, ColumnPath<C> path) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void shutdown() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public <K, C> CounterMutation<K, C> prepareCounterMutation(
			ColumnFamily<K, C> columnFamily, K rowKey, ColumnPath<C> path,
			long amount) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void start() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public <K, C> Query<K, C, Rows<K, C>> prepareCqlQuery(String cql) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, C> ColumnFamilyQuery<K, C> prepareQuery(ColumnFamily<K, C> cf) {
		// TODO Auto-generated method stub
		return null;
	}

}
