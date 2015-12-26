package com.netflix.astyanax.cql.direct;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.cql.CqlOperationResultImpl;
import com.netflix.astyanax.cql.CqlPreparedStatement;
import com.netflix.astyanax.cql.CqlStatementResult;
import com.netflix.astyanax.cql.util.AsyncOperationResult;

/**
 * Impl of {@link CqlPreparedStatement} using java driver.
 * it manages a {@link Session} object that is used when actually performing the real query with the 
 * driver underneath. 
 * 
 * @author poberai
 */
public class DirectCqlPreparedStatement implements CqlPreparedStatement {

	private final Session session;
	private final PreparedStatement pStmt; 
	private final List<Object> bindValues = new ArrayList<Object>(); 
	
	public DirectCqlPreparedStatement(Session session, PreparedStatement pStmt) {
		this.session = session;
		this.pStmt = pStmt;
	}

	@Override
	public OperationResult<CqlStatementResult> execute() throws ConnectionException {
		
		BoundStatement bStmt = pStmt.bind(bindValues.toArray());
		ResultSet resultSet = session.execute(bStmt);
		
		CqlStatementResult result = new DirectCqlStatementResultImpl(resultSet);
		return new CqlOperationResultImpl<CqlStatementResult>(resultSet, result);
	}

	@Override
	public ListenableFuture<OperationResult<CqlStatementResult>> executeAsync() throws ConnectionException {

		BoundStatement bStmt = pStmt.bind(bindValues.toArray());
		ResultSetFuture rsFuture = session.executeAsync(bStmt);

		return new AsyncOperationResult<CqlStatementResult>(rsFuture) {

			@Override
			public OperationResult<CqlStatementResult> getOperationResult(ResultSet rs) {
				CqlStatementResult result = new DirectCqlStatementResultImpl(rs);
				return new CqlOperationResultImpl<CqlStatementResult>(rs, result);			}
		};
	}

	@Override
	public <V> CqlPreparedStatement withByteBufferValue(V value, Serializer<V> serializer) {
		bindValues.add(value);
		return this;
	}

	@Override
	public CqlPreparedStatement withValue(ByteBuffer value) {
		bindValues.add(value);
		return this;
	}

	@Override
	public CqlPreparedStatement withValues(List<ByteBuffer> values) {
		bindValues.addAll(values);
		return this;
	}

	@Override
	public CqlPreparedStatement withStringValue(String value) {
		bindValues.add(value);
		return this;
	}

	@Override
	public CqlPreparedStatement withIntegerValue(Integer value) {
		bindValues.add(value);
		return this;
	}

	@Override
	public CqlPreparedStatement withBooleanValue(Boolean value) {
		bindValues.add(value);
		return this;
	}

	@Override
	public CqlPreparedStatement withDoubleValue(Double value) {
		bindValues.add(value);
		return this;
	}

	@Override
	public CqlPreparedStatement withLongValue(Long value) {
		bindValues.add(value);
		return this;
	}

	@Override
	public CqlPreparedStatement withFloatValue(Float value) {
		bindValues.add(value);
		return this;
	}

	@Override
	public CqlPreparedStatement withShortValue(Short value) {
		bindValues.add(value);
		return this;
	}

	@Override
	public CqlPreparedStatement withUUIDValue(UUID value) {
		bindValues.add(value);
		return this;
	}
	
	public PreparedStatement getInnerPreparedStatement() {
		return pStmt;
	}
}
