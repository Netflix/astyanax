package com.netflix.astyanax.entitystore;

import java.lang.reflect.Field;
import java.util.Map;

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
 * @param <T> entity type 
 * @param <K> rowKey type
 */
public class EntityPersister<T, K> {

	//////////////////////////////////////////////////////////////////
	// Builder pattern

	public static class Builder<T, K> {

		private Class<T> clazz = null;
		private EntityAnnotation entityAnnotation = null;
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
			// TODO: check @Id type compatibility
			// TODO: do we need to require @Entity annotation
			this.entityAnnotation = new EntityAnnotation(clazz);
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

		public EntityPersister<T, K> build() {
			// check mandatory fields
			Preconditions.checkNotNull(clazz, "clazz is not set");
			Preconditions.checkNotNull(keyspace, "keyspace is not set");
			Preconditions.checkNotNull(columnFamily, "columnFamily is not set");
			// build object
			return new EntityPersister<T, K>(this);
		}
	}

	//////////////////////////////////////////////////////////////////
	// private members

	private final Class<T> clazz;
	private final EntityAnnotation entityAnnotation;
	private final Keyspace keyspace;
	private final ColumnFamily<K, String> columnFamily;
	private final ConsistencyLevel readConsitency;
	private final ConsistencyLevel writeConsistency;
	private final Integer ttl;
	private final RetryPolicy retryPolicy;

	private EntityPersister(Builder<T, K> builder) {
		clazz = builder.clazz;
		entityAnnotation = builder.entityAnnotation;
		keyspace = builder.keyspace;
		columnFamily = builder.columnFamily;
		readConsitency = builder.readConsitency;
		writeConsistency = builder.writeConsistency;
		ttl = builder.ttl;
		retryPolicy = builder.retryPolicy;
	}

	private Integer getTTL(Field field) {
		// if there is no field level @TTL annotation,
		// return the default global value (which could also be valid null).
		TTL ttlAnnotation = field.getAnnotation(TTL.class);
		if(ttlAnnotation != null) {
			return ttlAnnotation.value();
		} else {
			return this.ttl;
		}
	}

	//////////////////////////////////////////////////////////////////
	// public APIs

	/**
	 * write entity to cassandra with mapped rowId and columns
	 * @param entity entity object
	 */
	public void put(T entity) throws PersistenceException {
		try {
			MutationBatch mb = keyspace.prepareMutationBatch();
			if(writeConsistency != null)
				mb.withConsistencyLevel(writeConsistency);
			if(retryPolicy != null)
				mb.withRetryPolicy(retryPolicy);

			Field idField = entityAnnotation.getId();
			@SuppressWarnings("unchecked")
			K rowKey = (K) idField.get(entity);
			ColumnListMutation<String> clm = mb.withRow(columnFamily, rowKey);

			Map<String, Field> columns = entityAnnotation.getColumns();
			for (Map.Entry<String, Field> entry : columns.entrySet()) {
				Coercions.setColumnMutationFromField(entity, entry.getValue(), entry.getKey(), clm, getTTL(entry.getValue()));
			}

			mb.execute();
		} catch(Exception e) {
			throw new PersistenceException("failed to write entity", e);
		}
	}

	/**
	 * fetch whole row and construct entity object mapping from columns
	 * @param id row key
	 * @return entity object
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

			T entity = clazz.newInstance();
			Field idField = entityAnnotation.getId();
			idField.set(entity, id);

			Map<String, Field> columns = entityAnnotation.getColumns();
			for (com.netflix.astyanax.model.Column<String> c : cl) {
				Field field = columns.get(c.getName());
				if (field != null) {
					Coercions.setFieldFromColumn(entity, field, c);
				}
			}
			return entity;
		} catch(Exception e) {
			throw new PersistenceException("failed to write entity", e);
		}
	}

	/**
	 * delete the whole row
	 * @param id row key
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
			throw new PersistenceException("failed to write entity", e);
		}
	}
}
