package com.netflix.astyanax.cql.writes;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.netflix.astyanax.CassandraOperationType;
import com.netflix.astyanax.ColumnMutation;
import com.netflix.astyanax.Execution;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.cql.CqlAbstractExecutionImpl;
import com.netflix.astyanax.cql.CqlKeyspaceImpl.KeyspaceContext;
import com.netflix.astyanax.cql.schema.CqlColumnFamilyDefinitionImpl;
import com.netflix.astyanax.cql.writes.CqlColumnListMutationImpl.ColumnFamilyMutationContext;
import com.netflix.astyanax.model.ColumnFamily;
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
	protected final ColumnFamilyMutationContext<K,C> cfContext;
	protected final Object columnName;

	// Tracking state
	protected Object columnValue;
	protected boolean counterColumn = false;
	protected boolean deleteColumn = false;
	
	private ConsistencyLevel consistencyLevel;
	private final AtomicReference<Long> timestamp = new AtomicReference<Long>(null);
	private final AtomicReference<Integer> ttl = new AtomicReference<Integer>(null);
	
	private final CFMutationQueryGenerator queryGen; 
	
	public CqlColumnMutationImpl(KeyspaceContext ksCtx, ColumnFamilyMutationContext<K,C> cfCtx, Object cName) {
		this.ksContext = ksCtx;
		this.cfContext = cfCtx;
		this.columnName = cName;
		
		this.queryGen = new CFMutationQueryGenerator(ksContext, cfContext, new ArrayList<CqlColumnMutationImpl<?,?>>(), 
				new AtomicReference<Boolean>(false), ttl, timestamp, consistencyLevel);
	}

	@Override
	public ColumnMutation setConsistencyLevel(ConsistencyLevel consistencyLevel) {
		this.consistencyLevel = consistencyLevel;
		return this;
	}

	@Override
	public ColumnMutation withRetryPolicy(RetryPolicy retry) {
		this.cfContext.setRetryPolicy(retry.duplicate());
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
		
		ColumnFamily<K,C> cf = cfContext.getColumnFamily();
		CqlColumnFamilyDefinitionImpl cfDef = (CqlColumnFamilyDefinitionImpl) cf.getColumnFamilyDefinition();
		if (cfDef.getPartitionKeyColumnDefinitionList().size() == 1) {
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
		this.counterColumn = true;
		return exec(amount, null, CassandraOperationType.COUNTER_MUTATE);
	}

	@Override
	public Execution<Void> deleteColumn() {
		deleteColumn = true;
		return exec(null, null, CassandraOperationType.COLUMN_DELETE);
	}

	@Override
	public Execution<Void> deleteCounterColumn() {
		deleteColumn = true;
		return exec(null, null, CassandraOperationType.COLUMN_DELETE);
	}

	private Execution<Void> exec(final Object value, final Integer overrideTTL, final CassandraOperationType opType) {

		final CqlColumnMutationImpl<?,?> colMutation = this;
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
			public Query getQuery() {
				BatchedStatements statements = new BatchedStatements();
				queryGen.appendQuery(statements, colMutation);
						
				String query = statements.getBatchQuery(false);
				
				PreparedStatement pStmt = ksContext.getSession().prepare(query);
				BoundStatement bStmt = pStmt.bind(statements.getBatchValues().toArray());
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
}
