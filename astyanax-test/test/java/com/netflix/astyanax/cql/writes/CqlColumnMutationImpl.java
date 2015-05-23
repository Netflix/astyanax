package com.netflix.astyanax.cql.writes;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.netflix.astyanax.CassandraOperationType;
import com.netflix.astyanax.ColumnMutation;
import com.netflix.astyanax.Execution;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.cql.ConsistencyLevelMapping;
import com.netflix.astyanax.cql.CqlAbstractExecutionImpl;
import com.netflix.astyanax.cql.CqlKeyspaceImpl.KeyspaceContext;
import com.netflix.astyanax.cql.schema.CqlColumnFamilyDefinitionImpl;
import com.netflix.astyanax.cql.util.CFQueryContext;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.retry.RetryPolicy;
import com.netflix.astyanax.serializers.BooleanSerializer;
import com.netflix.astyanax.serializers.ByteBufferSerializer;
import com.netflix.astyanax.serializers.DateSerializer;
import com.netflix.astyanax.serializers.DoubleSerializer;
import com.netflix.astyanax.serializers.FloatSerializer;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.ShortSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.serializers.UUIDSerializer;

public class CqlColumnMutationImpl<K,C> implements ColumnMutation {

	protected final KeyspaceContext ksContext;
	protected final CFQueryContext<K,C> cfContext;
	protected final CqlColumnFamilyDefinitionImpl cfDef;
	
	protected final Object columnName;
	protected Object columnValue;

	// Tracking state
	public enum ColMutationType {
		UpdateColumn, CounterColumn, DeleteColumn;
	}
	private ColMutationType type = ColMutationType.UpdateColumn;

	private ConsistencyLevel consistencyLevel;
	private final AtomicReference<Long> timestamp = new AtomicReference<Long>(null);
	private final AtomicReference<Integer> ttl = new AtomicReference<Integer>(null);
	
	private final CFMutationQueryGen queryGen; 
	
	public CqlColumnMutationImpl(KeyspaceContext ksCtx, CFQueryContext<K,C> cfCtx, Object cName) {
		this.ksContext = ksCtx;
		this.columnName = cName;
		this.cfContext = cfCtx;
		this.cfDef = (CqlColumnFamilyDefinitionImpl) cfContext.getColumnFamily().getColumnFamilyDefinition();
		this.queryGen = cfDef.getMutationQueryGenerator();
	}

	@Override
	public ColumnMutation setConsistencyLevel(ConsistencyLevel consistencyLevel) {
		this.consistencyLevel = consistencyLevel;
		return this;
	}

	@Override
	public ColumnMutation withRetryPolicy(RetryPolicy retry) {
		this.cfContext.setRetryPolicy(retry);
		return this;
	}

	@Override
	public ColumnMutation withTimestamp(long newValue) {
		this.timestamp.set(newValue);
		return this;
	}

	@Override
	public Execution<Void> putValue(String value, Integer ttl) {
		return putValue(value, StringSerializer.get(), ttl);
	}

	@Override
	public Execution<Void> putValue(byte[] value, Integer ttl) {
		return exec(ByteBuffer.wrap(value), ttl, CassandraOperationType.COLUMN_MUTATE);
	}

	@Override
	public Execution<Void> putValue(byte value, Integer ttl) {
		byte[] bytes = new byte[1];
		bytes[0] = value;
		return exec(ByteBuffer.wrap(bytes), ttl, CassandraOperationType.COLUMN_MUTATE);
	}

	@Override
	public Execution<Void> putValue(short value, Integer ttl) {
		return putValue(value, ShortSerializer.get(), ttl);
	}

	@Override
	public Execution<Void> putValue(int value, Integer ttl) {
		return putValue(value, IntegerSerializer.get(), ttl);
	}

	@Override
	public Execution<Void> putValue(long value, Integer ttl) {
		return putValue(value, LongSerializer.get(), ttl);
	}

	@Override
	public Execution<Void> putValue(boolean value, Integer ttl) {
		return putValue(value, BooleanSerializer.get(), ttl);
	}

	@Override
	public Execution<Void> putValue(ByteBuffer value, Integer ttl) {
		return exec(value, ttl, CassandraOperationType.COLUMN_MUTATE);
	}

	@Override
	public Execution<Void> putValue(Date value, Integer ttl) {
		return putValue(value, DateSerializer.get(), ttl);
	}

	@Override
	public Execution<Void> putValue(float value, Integer ttl) {
		return putValue(value, FloatSerializer.get(), ttl);
	}

	@Override
	public Execution<Void> putValue(double value, Integer ttl) {
		return putValue(value, DoubleSerializer.get(), ttl);
	}

	@Override
	public Execution<Void> putValue(UUID value, Integer ttl) {
		return putValue(value, UUIDSerializer.get(), ttl);
	}

	@Override
	public <T> Execution<Void> putValue(T value, Serializer<T> serializer, Integer ttl) {
		
		if (cfDef.getClusteringKeyColumnDefinitionList().size() == 0) {
			return exec(value, ttl, CassandraOperationType.COLUMN_MUTATE);
		}
		
		if (cfContext.getColumnFamily().getDefaultValueSerializer().getComparatorType() == ByteBufferSerializer.get().getComparatorType()) {
			ByteBuffer valueBytes = ((value instanceof ByteBuffer) ? (ByteBuffer) value : serializer.toByteBuffer(value));
			return exec(valueBytes, ttl, CassandraOperationType.COLUMN_MUTATE);
		} else {
			return exec(value, ttl, CassandraOperationType.COLUMN_MUTATE);
		}
	}
	
	public Execution<Void> putGenericValue(Object value, Integer ttl) {
		return exec(value, ttl, CassandraOperationType.COLUMN_MUTATE);
	}

	@Override
	public Execution<Void> putEmptyColumn(Integer ttl) {
		return exec(null, ttl, CassandraOperationType.COLUMN_MUTATE);
	}

	@Override
	public Execution<Void> incrementCounterColumn(long amount) {
		type = ColMutationType.CounterColumn;
		return exec(amount, null, CassandraOperationType.COUNTER_MUTATE);
	}

	@Override
	public Execution<Void> deleteColumn() {
		type = ColMutationType.DeleteColumn;
		return exec(null, null, CassandraOperationType.COLUMN_DELETE);
	}

	@Override
	public Execution<Void> deleteCounterColumn() {
		type = ColMutationType.DeleteColumn;
		return exec(null, null, CassandraOperationType.COLUMN_DELETE);
	}

	private Execution<Void> exec(final Object value, final Integer overrideTTL, final CassandraOperationType opType) {

		final CqlColumnMutationImpl<K, C> thisMutation = this;
		this.columnValue = value;
		if (overrideTTL != null) {
			this.ttl.set(overrideTTL);
		}
		
		return new CqlAbstractExecutionImpl<Void>(ksContext, cfContext) {

			@Override
			public CassandraOperationType getOperationType() {
				return opType;
			}

			@Override
			public Statement getQuery() {
				BoundStatement bStmt = queryGen.getColumnMutationStatement(thisMutation, false);
				if (thisMutation.getConsistencyLevel() != null) {
					bStmt.setConsistencyLevel(ConsistencyLevelMapping.getCL(getConsistencyLevel()));
				}
				return bStmt;
			}

			@Override
			public Void parseResultSet(ResultSet resultSet) {
				return null;
			}
		};
	}
	
	public Integer getTTL() {
		return ttl.get();
	}

	public Long getTimestamp() {
		return timestamp.get();
	}
	
	public String toString() {
		return columnName.toString();
	}

	public ColMutationType getType() {
		return type;
	}
	
	public Object getRowKey() {
		return cfContext.getRowKey();
	}
	
	public ConsistencyLevel getConsistencyLevel() {
		return this.consistencyLevel;
	}
}
