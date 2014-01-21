package com.netflix.astyanax.entitystore;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.persistence.PersistenceException;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.recipes.reader.AllRowsReader;
import com.netflix.astyanax.retry.RetryPolicy;
import com.netflix.astyanax.serializers.StringSerializer;

/**
 * Manager entities in a column famliy with any key type but columns that are
 * encoded as strings.
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
		private LifecycleEvents<T> lifecycleHandler = null;
		private String columnFamilyName = null;
		private boolean autoCommit = true;
		
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
		 * optional
		 * @param columnFamily column name type is fixed to String/UTF8
		 */
		public Builder<T, K> withColumnFamily(ColumnFamily<K, String> columnFamily) {
		    Preconditions.checkState(this.columnFamilyName == null && this.columnFamily == null , "withColumnFamily called multiple times");
			Preconditions.checkNotNull(columnFamily);
			this.columnFamily = columnFamily;
			return this;
		}
		
		/**
		 * optional
		 * @param columnFamilyName Name of column family to use.  
		 */
		public Builder<T, K> withColumnFamily(String columnFamilyName) {
            Preconditions.checkState(this.columnFamilyName == null && columnFamily == null , "withColumnFamily called multiple times");
            Preconditions.checkNotNull(columnFamilyName);
		    this.columnFamilyName = columnFamilyName;
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
		
		public Builder<T, K> withAutoCommit(boolean autoCommit) {
		    this.autoCommit = autoCommit;
		    return this;
		}

		@SuppressWarnings("unchecked")
        public DefaultEntityManager<T, K> build() {
			// check mandatory fields
			Preconditions.checkNotNull(clazz, "withEntityType(...) is not set");
			Preconditions.checkNotNull(keyspace, "withKeyspace(...) is not set");
			
			// TODO: check @Id type compatibility
			// TODO: do we need to require @Entity annotation
			this.entityMapper = new EntityMapper<T,K>(clazz, ttl);
			this.lifecycleHandler = new LifecycleEvents<T>(clazz);

			if (columnFamily == null) {
    			if (columnFamilyName == null)
    			    columnFamilyName = entityMapper.getEntityName();
    			columnFamily = new ColumnFamily<K, String>(
    			        columnFamilyName, 
    			        (com.netflix.astyanax.Serializer<K>)MappingUtils.getSerializerForField(this.entityMapper.getId()), 
    			        StringSerializer.get());
			}
			// build object
			return new DefaultEntityManager<T, K>(this);
		}
	}
	
	public static <T,K> Builder<T,K> builder() {
	    return new Builder<T,K>();
	}

	//////////////////////////////////////////////////////////////////
	// private members

	private final EntityMapper<T,K>        entityMapper;
	private final Keyspace                 keyspace;
	private final ColumnFamily<K, String>  columnFamily;
	private final ConsistencyLevel         readConsitency;
	private final ConsistencyLevel         writeConsistency;
	private final RetryPolicy              retryPolicy;
	private final LifecycleEvents<T>       lifecycleHandler;
	private final boolean                  autoCommit;
	private final ThreadLocal<MutationBatch> tlMutation = new ThreadLocal<MutationBatch>();
	
	private DefaultEntityManager(Builder<T, K> builder) {
		entityMapper      = builder.entityMapper;
		keyspace          = builder.keyspace;
		columnFamily      = builder.columnFamily;
		readConsitency    = builder.readConsitency;
		writeConsistency  = builder.writeConsistency;
		retryPolicy       = builder.retryPolicy;
		lifecycleHandler  = builder.lifecycleHandler;
		autoCommit        = builder.autoCommit;
	}

	//////////////////////////////////////////////////////////////////
	// public APIs

	/**
	 * @inheritDoc
	 */
	public void put(T entity) throws PersistenceException {
		try {
		    lifecycleHandler.onPrePersist(entity);
            MutationBatch mb = newMutationBatch();
			entityMapper.fillMutationBatch(mb, columnFamily, entity);			
            if (autoCommit)
                mb.execute();
            lifecycleHandler.onPostPersist(entity);
		} catch(Exception e) {
			throw new PersistenceException("failed to put entity ", e);
		}
	}

	/**
	 * @inheritDoc
	 */
	public T get(K id) throws PersistenceException {
		try {
			ColumnFamilyQuery<K, String> cfq = newQuery();            
			ColumnList<String> cl = cfq.getKey(id).execute().getResult();
			// when a row is deleted in cassandra,
			// the row key remains (without any columns) until the next compaction.
			// simply return null (as non exist)
			if(cl.isEmpty())
				return null;
			T entity = entityMapper.constructEntity(id, cl);
			lifecycleHandler.onPostLoad(entity);
			return entity;
		} catch(Exception e) {
			throw new PersistenceException("failed to get entity " + id, e);
		}
	}

	/**
	 * @inheritDoc
	 */
	@Override
	public void delete(K id) throws PersistenceException {
		try {
			MutationBatch mb = getMutationBatch();
			mb.withRow(columnFamily, id).delete();
            if (autoCommit)
                mb.execute();
		} catch(Exception e) {
			throw new PersistenceException("failed to delete entity " + id, e);
		}
	}
	
    @Override
    public void remove(T entity) throws PersistenceException {
        K id = null;
        try {
            lifecycleHandler.onPreRemove(entity);
            id = entityMapper.getEntityId(entity);
            MutationBatch mb = newMutationBatch();
            mb.withRow(columnFamily, id).delete();
            if (autoCommit)
                mb.execute();
            lifecycleHandler.onPostRemove(entity);
        } catch(Exception e) {
            throw new PersistenceException("failed to delete entity " + id, e);
        }
    }

    /**
     * @inheritDoc
     */
    @Override
    public List<T> getAll() throws PersistenceException {
        final List<T> entities = Lists.newArrayList();
        visitAll(new Function<T, Boolean>() {
            @Override
            public synchronized Boolean apply(T entity) {
                entities.add(entity);
                return true;
            }
        });
        return entities;
    }

    /**
     * @inheritDoc
     */
    @Override
    public List<T> get(Collection<K> ids) throws PersistenceException {
        try {
            ColumnFamilyQuery<K, String> cfq = newQuery();            
            Rows<K, String> rows = cfq.getRowSlice(ids).execute().getResult();

            List<T> entities = Lists.newArrayListWithExpectedSize(rows.size());
            for (Row<K, String> row : rows) {
                if (!row.getColumns().isEmpty()) { 
                    T entity = entityMapper.constructEntity(row.getKey(), row.getColumns());
                    lifecycleHandler.onPostLoad(entity);
                    entities.add(entity);
                }
            }
            return entities;
        } catch(Exception e) {
            throw new PersistenceException("failed to get entities " + ids, e);
        }
    }

    /**
     * @inheritDoc
     */
    @Override
    public void delete(Collection<K> ids) throws PersistenceException {
        MutationBatch mb = getMutationBatch();        
        try {
            for (K id : ids) {
                mb.withRow(columnFamily, id).delete();
            }
            if (autoCommit)
                mb.execute();
        } catch(Exception e) {
            throw new PersistenceException("failed to delete entities " + ids, e);
        }
    }

    @Override
    public void remove(Collection<T> entities) throws PersistenceException {
        MutationBatch mb = getMutationBatch();        
        try {
            for (T entity : entities) {
                lifecycleHandler.onPreRemove(entity);
                K id = entityMapper.getEntityId(entity);
                mb.withRow(columnFamily, id).delete();
            }
            mb.execute();
            for (T entity : entities) {
                lifecycleHandler.onPostRemove(entity);
            }
        } catch(Exception e) {
            throw new PersistenceException("failed to delete entities ", e);
        }
    }

    /**
     * @inheritDoc
     */
    @Override
    public void put(Collection<T> entities) throws PersistenceException {
        MutationBatch mb = getMutationBatch();        
        try {
            for (T entity : entities) {
                lifecycleHandler.onPrePersist(entity);
                entityMapper.fillMutationBatch(mb, columnFamily, entity);           
            }
            if (autoCommit)
                mb.execute();
            
            for (T entity : entities) {
                lifecycleHandler.onPostPersist(entity);
            }

        } catch(Exception e) {
            throw new PersistenceException("failed to put entities ", e);
        }
    }
    
    /**
     * @inheritDoc
     */
    @Override
    public void visitAll(final Function<T, Boolean> callback) throws PersistenceException {
        try {
            new AllRowsReader.Builder<K, String>(keyspace, columnFamily)
                    .withIncludeEmptyRows(false)
                    .forEachRow(new Function<Row<K,String>, Boolean>() {
                        @Override
                        public Boolean apply(Row<K, String> row) {
                            if (row.getColumns().isEmpty())
                                return true;
                            T entity = entityMapper.constructEntity(row.getKey(), row.getColumns());
                            try {
                                lifecycleHandler.onPostLoad(entity);
                            } catch (Exception e) {
                                // TODO:
                            }
                            return callback.apply(entity);
                        }
                    })
                    .build()
                    .call();
        } catch (Exception e) {
            throw new PersistenceException("Failed to fetch all entites", e);
        }
    }
    
    @Override
    public List<T> find(String cql) throws PersistenceException {
        Preconditions.checkArgument(StringUtils.left(cql, 6).equalsIgnoreCase("SELECT"), "CQL must be SELECT statement");
        
        try {
            CqlResult<K, String> results = newQuery().withCql(cql).execute().getResult();
            List<T> entities = Lists.newArrayListWithExpectedSize(results.getRows().size());
            for (Row<K, String> row : results.getRows()) {
                if (!row.getColumns().isEmpty()) { 
                    T entity = entityMapper.constructEntity(row.getKey(), row.getColumns());
                    lifecycleHandler.onPostLoad(entity);
                    entities.add(entity);
                }
            }
            return entities;
        } catch (Exception e) {
            throw new PersistenceException("Failed to execute cql query", e);
        }
    }
    
    private MutationBatch newMutationBatch() {
        MutationBatch mb = keyspace.prepareMutationBatch();
        if(writeConsistency != null)
            mb.withConsistencyLevel(writeConsistency);
        if(retryPolicy != null)
            mb.withRetryPolicy(retryPolicy);
        return mb;
    }
    
    private MutationBatch getMutationBatch() {
        if (autoCommit) {
            return newMutationBatch();
        }
        else {
            MutationBatch mb = tlMutation.get();
            if (mb == null) {
                mb = newMutationBatch();
                tlMutation.set(mb);
            }
            return mb;
        }
    }
    
    private ColumnFamilyQuery<K, String> newQuery() {
        ColumnFamilyQuery<K, String> cfq = keyspace.prepareQuery(columnFamily);
        if(readConsitency != null)
            cfq.setConsistencyLevel(readConsitency);
        if(retryPolicy != null)
            cfq.withRetryPolicy(retryPolicy);
        return cfq;
    }

    @Override
    public void createStorage(Map<String, Object> options) throws PersistenceException {
        try {
            keyspace.createColumnFamily(this.columnFamily, options);
        } catch (ConnectionException e) {
            if (e.getMessage().contains("already exist")) 
                return;
            throw new PersistenceException("Unable to create column family " + this.columnFamily.getName(), e);
        }
    }

    @Override
    public void deleteStorage() throws PersistenceException {
        try {
            keyspace.dropColumnFamily(this.columnFamily);
        } catch (ConnectionException e) {
            throw new PersistenceException("Unable to drop column family " + this.columnFamily.getName(), e);
        }
    }

    @Override
    public void truncate() throws PersistenceException {
        try {
            keyspace.truncateColumnFamily(this.columnFamily);
        } catch (ConnectionException e) {
            throw new PersistenceException("Unable to drop column family " + this.columnFamily.getName(), e);
        }
    }

    @Override
    public void commit() throws PersistenceException {
        MutationBatch mb = tlMutation.get();
        if (mb != null) {
            try {
                mb.execute();
            } catch (ConnectionException e) {
                throw new PersistenceException("Failed to commit mutation batch", e);
            }
        }
    }

    @Override
    public NativeQuery<T, K> createNativeQuery() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

}
