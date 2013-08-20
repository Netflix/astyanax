package com.netflix.astyanax.cql.reads;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.cql.CqlOperationResultImpl;
import com.netflix.astyanax.cql.util.ChainedContext;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.query.CqlQuery;
import com.netflix.astyanax.query.PreparedCqlQuery;

public class DirectCqlQueryImpl<K, C> implements CqlQuery<K, C> {

	private Cluster cluster;
	private String basicCqlQuery; 
	
	
	public DirectCqlQueryImpl(ChainedContext context, String basicCqlQuery) {
		context.rewindForRead();
		this.cluster = context.getNext(Cluster.class);
		this.basicCqlQuery = basicCqlQuery;
	}
	
	@Override
	public OperationResult<CqlResult<K, C>> execute() throws ConnectionException {
		ResultSet rs = cluster.connect().execute(basicCqlQuery);
		return processResult(rs);
	}

	@Override
	public ListenableFuture<OperationResult<CqlResult<K, C>>> executeAsync() throws ConnectionException {
		throw new NotImplementedException();
	}
	
	private CqlOperationResultImpl<CqlResult<K, C>> processResult(ResultSet rs) throws ConnectionException {
		boolean isCountQuery = basicCqlQuery.contains(" count(");
		return new CqlOperationResultImpl<CqlResult<K,C>>(rs, new DirectCqlResult<K, C>(isCountQuery, rs));
	}

	@Override
	public CqlQuery<K, C> useCompression() {
		throw new NotImplementedException();
	}

	@Override
	public PreparedCqlQuery<K, C> asPreparedStatement() {
		
		final Session session = cluster.connect();
		final BoundStatement boundStatement = new BoundStatement(cluster.connect().prepare(basicCqlQuery));
		final List<Object> bindList = new ArrayList<Object>();
		
		return new PreparedCqlQuery<K, C>() {

			@Override
			public OperationResult<CqlResult<K, C>> execute() throws ConnectionException {
				
				ResultSet rs = session.execute(boundStatement.bind(bindList.toArray()));
				return processResult(rs);
			}

			@Override
			public ListenableFuture<OperationResult<CqlResult<K, C>>> executeAsync() throws ConnectionException {
				throw new NotImplementedException();
			}

			@Override
			public <V> PreparedCqlQuery<K, C> withByteBufferValue(V value, Serializer<V> serializer) {
				throw new NotImplementedException();
			}

			@Override
			public PreparedCqlQuery<K, C> withValue(ByteBuffer value) {
				throw new NotImplementedException();
			}

			@Override
			public PreparedCqlQuery<K, C> withValues(List<ByteBuffer> value) {
				throw new NotImplementedException();
			}

			@Override
			public PreparedCqlQuery<K, C> withStringValue(String value) {
				bindList.add(value);
				return this;
			}

			@Override
			public PreparedCqlQuery<K, C> withIntegerValue(Integer value) {
				bindList.add(value);
				return this;
			}

			@Override
			public PreparedCqlQuery<K, C> withBooleanValue(Boolean value) {
				bindList.add(value);
				return this;
			}

			@Override
			public PreparedCqlQuery<K, C> withDoubleValue(Double value) {
				bindList.add(value);
				return this;
			}

			@Override
			public PreparedCqlQuery<K, C> withLongValue(Long value) {
				bindList.add(value);
				return this;
			}

			@Override
			public PreparedCqlQuery<K, C> withFloatValue(Float value) {
				bindList.add(value);
				return this;
			}

			@Override
			public PreparedCqlQuery<K, C> withShortValue(Short value) {
				bindList.add(value);
				return this;
			}

			@Override
			public PreparedCqlQuery<K, C> withUUIDValue(UUID value) {
				bindList.add(value);
				return this;
			}
		};
	}
}
