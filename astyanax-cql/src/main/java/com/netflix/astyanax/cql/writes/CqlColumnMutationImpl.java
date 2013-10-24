package com.netflix.astyanax.cql.writes;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.netflix.astyanax.cql.util.ConsistencyLevelTransform;
import com.netflix.astyanax.cql.writes.CqlColumnListMutationImpl.ColumnFamilyMutationContext;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.retry.RetryPolicy;
import com.netflix.astyanax.serializers.ByteBufferSerializer;

public class CqlColumnMutationImpl<K,C> implements ColumnMutation {

	private static final Logger LOG = LoggerFactory.getLogger(CqlColumnMutationImpl.class);
	
	protected final KeyspaceContext ksContext;
	protected final ColumnFamilyMutationContext<K,C> cfContext;
	protected final Object columnName;

	// Tracking state
	protected Object columnValue;
	protected boolean counterColumn = false;
	protected boolean deleteColumn = false;
	
	private ConsistencyLevel consistencyLevel;
	private Long timestamp;
	private Integer ttl;
	
	public CqlColumnMutationImpl(KeyspaceContext ksCtx, ColumnFamilyMutationContext<K,C> cfCtx, Object cName) {
		this.ksContext = ksCtx;
		this.cfContext = cfCtx;
		this.columnName = cName;
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
	public ColumnMutation withTimestamp(long timestamp) {
		this.timestamp = timestamp;
		return this;
	}

	@Override
	public Execution<Void> putValue(String value, Integer ttl) {
		return exec(value, ttl, CassandraOperationType.COLUMN_MUTATE);
	}

	@Override
	public Execution<Void> putValue(byte[] value, Integer ttl) {
		return exec(value, ttl, CassandraOperationType.COLUMN_MUTATE);
	}

	@Override
	public Execution<Void> putValue(byte value, Integer ttl) {
		return exec(value, ttl, CassandraOperationType.COLUMN_MUTATE);
	}

	@Override
	public Execution<Void> putValue(short value, Integer ttl) {
		return exec(value, ttl, CassandraOperationType.COLUMN_MUTATE);
	}

	@Override
	public Execution<Void> putValue(int value, Integer ttl) {
		return exec(value, ttl, CassandraOperationType.COLUMN_MUTATE);
	}

	@Override
	public Execution<Void> putValue(long value, Integer ttl) {
		return exec(value, ttl, CassandraOperationType.COLUMN_MUTATE);
	}

	@Override
	public Execution<Void> putValue(boolean value, Integer ttl) {
		return exec(value, ttl, CassandraOperationType.COLUMN_MUTATE);
	}

	@Override
	public Execution<Void> putValue(ByteBuffer value, Integer ttl) {
		return exec(value, ttl, CassandraOperationType.COLUMN_MUTATE);
	}

	@Override
	public Execution<Void> putValue(Date value, Integer ttl) {
		return exec(value, ttl, CassandraOperationType.COLUMN_MUTATE);
	}

	@Override
	public Execution<Void> putValue(float value, Integer ttl) {
		return exec(value, ttl, CassandraOperationType.COLUMN_MUTATE);
	}

	@Override
	public Execution<Void> putValue(double value, Integer ttl) {
		return exec(value, ttl, CassandraOperationType.COLUMN_MUTATE);
	}

	@Override
	public Execution<Void> putValue(UUID value, Integer ttl) {
		return exec(value, ttl, CassandraOperationType.COLUMN_MUTATE);
	}

	@Override
	public <T> Execution<Void> putValue(T value, Serializer<T> serializer, Integer ttl) {
		
		ColumnFamily<K,C> cf = cfContext.getColumnFamily();
		CqlColumnFamilyDefinitionImpl cfDef = (CqlColumnFamilyDefinitionImpl) cf.getColumnFamilyDefinition();
		if (cfDef.getPartitionKeyColumnDefinitionList().size() == 1) {
			return exec(value, ttl, CassandraOperationType.COLUMN_MUTATE);
		}
		
		if (cfContext.getColumnFamily().getDefaultValueSerializer().getComparatorType() == ByteBufferSerializer.get().getComparatorType()) {
			ByteBuffer valueBytes = serializer.toByteBuffer(value);
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
		return exec(amount, ttl, CassandraOperationType.COUNTER_MUTATE);
	}

	@Override
	public Execution<Void> deleteColumn() {
		deleteColumn = true;
		return exec(null, ttl, CassandraOperationType.COLUMN_DELETE);
	}

	@Override
	public Execution<Void> deleteCounterColumn() {
		deleteColumn = true;
		return exec(null, ttl, CassandraOperationType.COLUMN_DELETE);
	}

	private Execution<Void> exec(Object value, final Integer ttl, final CassandraOperationType opType) {

		this.columnValue = value;
		this.ttl = ttl;

		return new CqlAbstractExecutionImpl<Void>(ksContext, cfContext) {

			@Override
			public CassandraOperationType getOperationType() {
				return opType;
			}

			@Override
			public Query getQuery() {

				StringBuilder sb = new StringBuilder("UPDATE ");
				sb.append( keyspace + "." + cf.getName());

				appendWriteOpts(sb);

				if (counterColumn) {
					getCounterColumnUpdate(sb);
				} else {
					getRegularColumnUpdate(sb);
				}

				sb.append(" WHERE key = ?");

				String query = sb.toString();
				
				if (LOG.isDebugEnabled()) {
					LOG.debug("UPDATE query: " + query);
				}

				PreparedStatement statement = session.prepare(query);
				BoundStatement boundStatement = new BoundStatement(statement);
				boundStatement.bind(columnValue, cfContext.getRowKey());

				return boundStatement;
			}

			@Override
			public Void parseResultSet(ResultSet resultSet) {
				return null;
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

			private void appendWriteOpts(StringBuilder sb) {
				appendWriteOptions(sb, timestamp, ttl, consistencyLevel);
			}

		};
	}

	public static void appendWriteOptions(StringBuilder sb, Long timestamp, Integer ttl, ConsistencyLevel consistencyLevel) {
		
		if (timestamp != null || ttl != null || consistencyLevel != null) {
			sb.append(" USING ");
		}
		
		if (ttl != null) {
			sb.append(" TTL " + ttl);
		}
		
		if (timestamp != null) {
			if (ttl != null) {
				sb.append(" AND");
			}
			sb.append(" TIMESTAMP " + timestamp);
		}
		
		if (consistencyLevel != null) {
			if (ttl != null || timestamp != null) {
				sb.append(" AND");
			}
			sb.append(" CONSISTENCY " + ConsistencyLevelTransform.getConsistencyLevel(consistencyLevel).name());
		}
	}
	
	public Integer getTTL() {
		return ttl;
	}

	public Long getTimestamp() {
		return timestamp;
	}
	
	public String toString() {
		return columnName.toString();
	}
}
