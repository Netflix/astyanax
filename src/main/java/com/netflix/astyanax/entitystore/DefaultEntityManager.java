package com.netflix.astyanax.entitystore;

import javax.persistence.PersistenceException;

import com.google.common.base.Preconditions;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.retry.RetryPolicy;

/**
 * Column name is String
 */
public class DefaultEntityManager<T, K> implements EntityManager<T, K> {

	//////////////////////////////////////////////////////////////////
	// Builder pattern

	public static class Builder<T, K> {

		private Class<T> clazz = null;
		private EntityMapper<T,K> entityMapper = null;
		private Keyspace keyspace = null;
		private ColumnFamily<K, String> columnFamily = null;
		private ConsistencyLevel readConsitency = null;
		private ConsistencyLevel writeConsistency = null;
		private Integer ttl = null;
		private RetryPolicy retryPolicy = null;

		public Builder() {

		}

		/**
		 * mandatory
		 * @param clazz entity class type
		 */
		public Builder<T, K> withEntityType(Class<T> clazz) {
			Preconditions.checkNotNull(clazz);
			this.clazz = clazz;
			return this;
		}

		/**
		 * mandatory
		 * @param keyspace
		 */
		public Builder<T, K> withKeyspace(Keyspace keyspace) {
			Preconditions.checkNotNull(keyspace);
			this.keyspace = keyspace;
			return this;
		}

		/**
		 * mandatory
		 * @param columnFamily column name type is fixed to String/UTF8
		 */
		public Builder<T, K> withColumnFamily(ColumnFamily<K, String> columnFamily) {
			Preconditions.checkNotNull(columnFamily);
			this.columnFamily = columnFamily;
			return this;
		}

		/**
		 * optional
		 * @param level
		 */
		public Builder<T, K> withReadConsistency(ConsistencyLevel level) {
			Preconditions.checkNotNull(level);
			this.readConsitency = level;
			return this;
		}

		/**
		 * optional
		 * @param level
		 */
		public Builder<T, K> withWriteConsistency(ConsistencyLevel level) {
			Preconditions.checkNotNull(level);
			this.writeConsistency = level;
			return this;
		}

		/**
		 * set both read and write consistency
		 * optional
		 * @param level
		 */
		public Builder<T, K> withConsistency(ConsistencyLevel level) {
			Preconditions.checkNotNull(level);
			this.readConsitency = level;
			this.writeConsistency = level;
			return this;
		}

		/**
		 * default TTL for all columns written to cassandra
		 * optional
		 * @return
		 */
		public Builder<T, K> withTTL(Integer ttl) {
			this.ttl = ttl;
			return this;
		}

		/**
		 * optional
		 * @param level
		 */
		public Builder<T, K> withRetryPolicy(RetryPolicy policy) {
			Preconditions.checkNotNull(policy);
			this.retryPolicy = policy;
			return this;
		}

		public DefaultEntityManager<T, K> build() {
			// check mandatory fields
			Preconditions.checkNotNull(clazz, "withEntityType(...) is not set");
			Preconditions.checkNotNull(keyspace, "withKeyspace(...) is not set");
			Preconditions.checkNotNull(columnFamily, "withColumnFamily(...) is not set");
			
			// TODO: check @Id type compatibility
			// TODO: do we need to require @Entity annotation
			this.entityMapper = new EntityMapper<T,K>(clazz, ttl);
			
			// build object
			return new DefaultEntityManager<T, K>(this);
		}
	}

	//////////////////////////////////////////////////////////////////
	// private members

	private final EntityMapper<T,K> entityMapper;
	private final Keyspace keyspace;
	private final ColumnFamily<K, String> columnFamily;
	private final ConsistencyLevel readConsitency;
	private final ConsistencyLevel writeConsistency;
	private final RetryPolicy retryPolicy;

	private DefaultEntityManager(Builder<T, K> builder) {
		entityMapper = builder.entityMapper;
		keyspace = builder.keyspace;
		columnFamily = builder.columnFamily;
		readConsitency = builder.readConsitency;
		writeConsistency = builder.writeConsistency;
		retryPolicy = builder.retryPolicy;
	}



	//////////////////////////////////////////////////////////////////
	// public APIs

	/**
	 * @inheritDoc
	 */
	public void put(T entity) throws PersistenceException {
		try {
			MutationBatch mb = keyspace.prepareMutationBatch();
			if(writeConsistency != null)
				mb.withConsistencyLevel(writeConsistency);
			if(retryPolicy != null)
				mb.withRetryPolicy(retryPolicy);

			entityMapper.fillMutationBatch(mb, columnFamily, entity);			
			mb.execute();
		} catch(Exception e) {
			throw new PersistenceException("failed to put entity", e);
		}
	}

	/**
	 * @inheritDoc
	 */
	public T get(K id) throws PersistenceException {
		try {
			ColumnFamilyQuery<K, String> cfq = keyspace.prepareQuery(columnFamily);
			if(readConsitency != null)
				cfq.setConsistencyLevel(readConsitency);
			if(retryPolicy != null)
				cfq.withRetryPolicy(retryPolicy);
			
			RowQuery<K, String> rq = cfq.getKey(id);
			ColumnList<String> cl = rq.execute().getResult();

			T entity = entityMapper.constructEntity(id, cl);
			return entity;
		} catch(Exception e) {
			throw new PersistenceException("failed to get entity", e);
		}
	}

	/**
	 * @inheritDoc
	 */
	public void delete(K id) throws PersistenceException {
		try {
			MutationBatch mb = keyspace.prepareMutationBatch();
			ColumnListMutation<String> clm = mb.withRow(columnFamily, id);
			clm.delete();
			if(writeConsistency != null)
				mb.withConsistencyLevel(writeConsistency);
			if(retryPolicy != null)
				mb.withRetryPolicy(retryPolicy);
			mb.execute();
		} catch(Exception e) {
			throw new PersistenceException("failed to delete entity", e);
		}
	}
}
