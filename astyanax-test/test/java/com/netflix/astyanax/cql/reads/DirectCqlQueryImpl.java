package com.netflix.astyanax.cql.reads;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang.NotImplementedException;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.CassandraOperationType;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.cql.CqlAbstractExecutionImpl;
import com.netflix.astyanax.cql.CqlKeyspaceImpl.KeyspaceContext;
import com.netflix.astyanax.cql.reads.model.DirectCqlResult;
import com.netflix.astyanax.cql.util.CFQueryContext;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.query.CqlQuery;
import com.netflix.astyanax.query.PreparedCqlQuery;

/**
 * 
 * Impl for {@link CqlQuery} that allows users to directly send CQL3 over java driver
 * @author poberai
 *
 * @param <K>
 * @param <C>
 */
public class DirectCqlQueryImpl<K, C> implements CqlQuery<K, C> {

	private final KeyspaceContext ksContext;
	private final CFQueryContext<K,C> cfContext;
	private final String basicCqlQuery; 
	
	public DirectCqlQueryImpl(KeyspaceContext ksCtx, CFQueryContext<K,C> cfCtx, String basicCqlQuery) {
		this.ksContext = ksCtx;
		this.cfContext = cfCtx;
		this.basicCqlQuery = basicCqlQuery;
	}
	
	@Override
	public OperationResult<CqlResult<K, C>> execute() throws ConnectionException {
		return new InternalExecutionImpl(new SimpleStatement(basicCqlQuery)).execute();
	}

	@Override
	public ListenableFuture<OperationResult<CqlResult<K, C>>> executeAsync() throws ConnectionException {
		return new InternalExecutionImpl(new SimpleStatement(basicCqlQuery)).executeAsync();
	}
	
	@Override
	public CqlQuery<K, C> useCompression() {
		throw new UnsupportedOperationException("Operation not supported");
	}

	protected class InternalPreparedStatement implements PreparedCqlQuery<K,C> {
		
		private final PreparedStatement pStatement;
		
		protected InternalPreparedStatement() {
			pStatement = ksContext.getSession().prepare(basicCqlQuery);
		}
		
		@Override
		public <V> PreparedCqlQuery<K, C> withByteBufferValue(V value, Serializer<V> serializer) {
			return new InternalBoundStatement(pStatement).withByteBufferValue(value, serializer);
		}

		@Override
		public PreparedCqlQuery<K, C> withValue(ByteBuffer value) {
			return new InternalBoundStatement(pStatement).withValue(value);
		}

		@Override
		public PreparedCqlQuery<K, C> withValues(List<ByteBuffer> values) {
			return new InternalBoundStatement(pStatement).withValues(values);
		}

		@Override
		public PreparedCqlQuery<K, C> withStringValue(String value) {
			return new InternalBoundStatement(pStatement).withStringValue(value);
		}

		@Override
		public PreparedCqlQuery<K, C> withIntegerValue(Integer value) {
			return new InternalBoundStatement(pStatement).withIntegerValue(value);
		}

		@Override
		public PreparedCqlQuery<K, C> withBooleanValue(Boolean value) {
			return new InternalBoundStatement(pStatement).withBooleanValue(value);
		}

		@Override
		public PreparedCqlQuery<K, C> withDoubleValue(Double value) {
			return new InternalBoundStatement(pStatement).withDoubleValue(value);
		}

		@Override
		public PreparedCqlQuery<K, C> withLongValue(Long value) {
			return new InternalBoundStatement(pStatement).withLongValue(value);
		}

		@Override
		public PreparedCqlQuery<K, C> withFloatValue(Float value) {
			return new InternalBoundStatement(pStatement).withFloatValue(value);
		}

		@Override
		public PreparedCqlQuery<K, C> withShortValue(Short value) {
			return new InternalBoundStatement(pStatement).withShortValue(value);
		}

		@Override
		public PreparedCqlQuery<K, C> withUUIDValue(UUID value) {
			return new InternalBoundStatement(pStatement).withUUIDValue(value);
		}

		@Override
		public OperationResult<CqlResult<K, C>> execute() throws ConnectionException {
			throw new NotImplementedException();
		}

		@Override
		public ListenableFuture<OperationResult<CqlResult<K, C>>> executeAsync() throws ConnectionException {
			throw new NotImplementedException();
		}
	}
	
	protected class InternalBoundStatement implements PreparedCqlQuery<K,C> {
		
		final List<Object> bindList = new ArrayList<Object>();
		final BoundStatement boundStatement;
		
		protected InternalBoundStatement(PreparedStatement pStmt) {
			 boundStatement = new BoundStatement(pStmt);
		}
		
		@Override
		public OperationResult<CqlResult<K, C>> execute() throws ConnectionException {
			boundStatement.bind(bindList.toArray());
			return new InternalExecutionImpl(boundStatement).execute();
		}

		@Override
		public ListenableFuture<OperationResult<CqlResult<K, C>>> executeAsync() throws ConnectionException {
			boundStatement.bind(bindList.toArray());
			return new InternalExecutionImpl(boundStatement).executeAsync();
		}

		@Override
		public <V> PreparedCqlQuery<K, C> withByteBufferValue(V value, Serializer<V> serializer) {
			bindList.add(value);
			return this;
		}

		@Override
		public PreparedCqlQuery<K, C> withValue(ByteBuffer value) {
			bindList.add(value);
			return this;
		}

		@Override
		public PreparedCqlQuery<K, C> withValues(List<ByteBuffer> value) {
			bindList.addAll(value);
			return this;
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
	}
	
	
	@Override
	public PreparedCqlQuery<K, C> asPreparedStatement() {
		return new InternalPreparedStatement();
	}
	
	private class InternalExecutionImpl extends CqlAbstractExecutionImpl<CqlResult<K, C>> {

		private final Statement query;
		
		public InternalExecutionImpl(Statement query) {
			super(ksContext, cfContext);
			this.query = query;
		}

		@Override
		public CassandraOperationType getOperationType() {
			return CassandraOperationType.CQL;
		}

		@Override
		public Statement getQuery() {
			return query;
		}

		@Override
		public CqlResult<K, C> parseResultSet(ResultSet resultSet) {
			
			boolean isCountQuery = basicCqlQuery.contains(" count(");
			if (isCountQuery) {
				return new DirectCqlResult<K,C>(new Long(resultSet.one().getLong(0)));
			} else {
				return new DirectCqlResult<K,C>(resultSet.all(), (ColumnFamily<K, C>) cf);
			}
		}
	}
}
