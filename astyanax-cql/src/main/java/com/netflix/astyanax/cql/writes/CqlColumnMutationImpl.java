package com.netflix.astyanax.cql.writes;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.ColumnMutation;
import com.netflix.astyanax.Execution;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.cql.CqlOperationResultImpl;
import com.netflix.astyanax.cql.util.ConsistencyLevelTransform;
import com.netflix.astyanax.cql.util.Context.ColumnFamilyCtx;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.retry.RetryPolicy;

public class CqlColumnMutationImpl implements ColumnMutation {
	
	private Cluster cluster;
	private String keyspace;
	private ColumnFamily<?,?> cf;
	private Object rowKey;

	protected String columnName;
	protected Object columnValue;
	protected boolean counterColumn = false;
	
	private ConsistencyLevel consistencyLevel;
	private Long timestamp;
	private Integer ttl;
	
	public CqlColumnMutationImpl(ColumnFamilyCtx ctx, String columnName, Object rowKey) {
		this.cluster = ctx.cluster;
		this.keyspace = ctx.keyspace;
		this.cf = ctx.columnFamily;
		this.columnName = columnName;
		this.rowKey = rowKey;
	}

	CqlColumnMutationImpl(String columnName) {
		this.columnName = columnName;
	}
	
	@Override
	public ColumnMutation setConsistencyLevel(ConsistencyLevel consistencyLevel) {
		this.consistencyLevel = consistencyLevel;
		return this;
	}

	@Override
	public ColumnMutation withRetryPolicy(RetryPolicy retry) {
		throw new NotImplementedException();
	}

	@Override
	public ColumnMutation withTimestamp(long timestamp) {
		this.timestamp = timestamp;
		return this;
	}

	@Override
	public Execution<Void> putValue(String value, Integer ttl) {
		return exec(value, ttl);
	}

	@Override
	public Execution<Void> putValue(byte[] value, Integer ttl) {
		return exec(value, ttl);
	}

	@Override
	public Execution<Void> putValue(byte value, Integer ttl) {
		return exec(value, ttl);
	}

	@Override
	public Execution<Void> putValue(short value, Integer ttl) {
		return exec(value, ttl);
	}

	@Override
	public Execution<Void> putValue(int value, Integer ttl) {
		return exec(value, ttl);
	}

	@Override
	public Execution<Void> putValue(long value, Integer ttl) {
		return exec(value, ttl);
	}

	@Override
	public Execution<Void> putValue(boolean value, Integer ttl) {
		return exec(value, ttl);
	}

	@Override
	public Execution<Void> putValue(ByteBuffer value, Integer ttl) {
		return exec(value, ttl);
	}

	@Override
	public Execution<Void> putValue(Date value, Integer ttl) {
		return exec(value, ttl);
	}

	@Override
	public Execution<Void> putValue(float value, Integer ttl) {
		return exec(value, ttl);
	}

	@Override
	public Execution<Void> putValue(double value, Integer ttl) {
		return exec(value, ttl);
	}

	@Override
	public Execution<Void> putValue(UUID value, Integer ttl) {
		return exec(value, ttl);
	}

	@Override
	public <T> Execution<Void> putValue(T value, Serializer<T> serializer, Integer ttl) {
		return exec(value, ttl);
	}

	@Override
	public Execution<Void> putEmptyColumn(Integer ttl) {
		return exec(null, ttl);
	}

	@Override
	public Execution<Void> incrementCounterColumn(long amount) {
		this.counterColumn = true;
		return exec(amount, ttl);
	}

	@Override
	public Execution<Void> deleteColumn() {
		return exec(null, ttl);
	}

	@Override
	public Execution<Void> deleteCounterColumn() {
		return exec(null, ttl);
	}

	
	private Execution<Void> exec(Object value, int ttl) {
		
		this.columnValue = value;
		this.ttl = ttl;

		return new Execution<Void>() {

			@Override
			public OperationResult<Void> execute() throws ConnectionException {
				
				ResultSet rs = cluster.connect().execute(getStatement());
				return new CqlOperationResultImpl<Void>(rs, null);
			}

			@Override
			public ListenableFuture<OperationResult<Void>> executeAsync() throws ConnectionException {
				throw new NotImplementedException();
			}
		};
	}
	
	public BoundStatement getStatement() {
		
		Preconditions.checkArgument(rowKey != null, "Row key must be provided");
		Preconditions.checkArgument(keyspace != null, "Keyspace must be provided");
		Preconditions.checkArgument(cf != null, "ColumnFamily must be provided");
		
		StringBuilder sb = new StringBuilder("UPDATE ");
		sb.append( keyspace + "." + cf.getName());
		
		appendWriteOptions(sb);
		
		if (counterColumn) {
			getCounterColumnUpdate(sb);
		} else {
			getRegularColumnUpdate(sb);
		}
		
		sb.append(" WHERE key = ?");

		String query = sb.toString();
		System.out.println("UPDATE query: " + query);
		
		PreparedStatement statement = cluster.connect().prepare(query);
		BoundStatement boundStatement = new BoundStatement(statement);
		boundStatement.bind(columnValue, rowKey);
		
		return boundStatement;
	}

	private void getCounterColumnUpdate(StringBuilder sb) {
		
		long increment = ((Long)columnValue).longValue();
		
		if (increment < 0) {
			sb.append(" SET ").append(columnName).append(" = ").append(columnName).append(" - ?");
			columnValue = Math.abs(increment);
		} else {
			sb.append(" SET ").append(columnName).append(" = ").append(columnName).append(" + ?");
		}
	}

	private void getRegularColumnUpdate(StringBuilder sb) {
		sb.append(" SET ").append(columnName).append(" = ?");
	}
	
	private void appendWriteOptions(StringBuilder sb) {
		appendWriteOptions(sb, timestamp, ttl, consistencyLevel);
	}

	public static void appendWriteOptions(StringBuilder sb, Long timestamp, Integer ttl, ConsistencyLevel consistencyLevel) {
	
		if (timestamp != null || ttl != null || consistencyLevel != null) {
			sb.append(" USING ");
		}
		
		boolean first = true;
		
		if (ttl != null) {
			sb.append(" TTL " + ttl);
			first = false;
		}
		
		if (timestamp != null) {
			if (!first) {
				sb.append(" AND");
			}
			sb.append(" TIMESTAMP " + timestamp);
			first = false;
		}
		
		if (consistencyLevel != null) {
			if (!first) {
				sb.append(" AND");
			}
			sb.append(" CONSISTENCY " + ConsistencyLevelTransform.getConsistencyLevel(consistencyLevel).name());
			first = false;
		}
	}
}
