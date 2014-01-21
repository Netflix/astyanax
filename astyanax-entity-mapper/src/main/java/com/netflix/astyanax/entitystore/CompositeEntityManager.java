package com.netflix.astyanax.entitystore;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.persistence.PersistenceException;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.MutationBatchManager;
import com.netflix.astyanax.ThreadLocalMutationBatchManager;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.query.RowSliceQuery;
import com.netflix.astyanax.recipes.reader.AllRowsReader;
import com.netflix.astyanax.retry.RetryPolicy;
import com.netflix.astyanax.serializers.ByteBufferSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.util.RangeBuilder;

/**
 * Entity manager for a composite column family.  This entity manager expects
 * the entity to have a single @Id which corresponds to the row key.  It will then
 * have at least 3 columns, with all but the last being parts of the composite.
 * 
 * @Entity
 * class Entity {
 *      @Id     String rowKey
 *      @Column String firstCompositePart;
 *      @Column Long   secondCompositePart;
 *      @Column String valuePart;
 * }
 * 
 * 
 * @author elandau
 *
 * @param <T>   Entity type
 * @param <K>   Partition key
 */
public class CompositeEntityManager<T, K> implements EntityManager<T, K> {
    private static final Logger LOG = LoggerFactory.getLogger(CompositeEntityManager.class);
    private static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.CL_ONE;
    
    public static class Builder<T, K> {
        private Keyspace                    keyspace;
        private Class<T>                    clazz;
        private ColumnFamily<K, ByteBuffer> columnFamily        = null;
        private ConsistencyLevel            readConsitency      = DEFAULT_CONSISTENCY_LEVEL;
        private ConsistencyLevel            writeConsistency    = DEFAULT_CONSISTENCY_LEVEL;
        private CompositeEntityMapper<T, K> entityMapper;
        private Integer                     ttl                 = null;
        private RetryPolicy                 retryPolicy         = null;
        private LifecycleEvents<T>          lifecycleHandler    = null;
        private String                      columnFamilyName    = null;
        private boolean                     autoCommit          = true;
        private MutationBatchManager        batchManager        = null;
        private boolean                     verbose             = false;
        private ByteBuffer                  prefix              = null;
        
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
         * @param columnFamilyName Name of column family to use.  
         */
        public Builder<T, K> withColumnFamily(String columnFamilyName) {
            Preconditions.checkState(this.columnFamilyName == null && columnFamily == null , "withColumnFamily called multiple times");
            Preconditions.checkNotNull(columnFamilyName);
            this.columnFamilyName = columnFamilyName; // .toLowerCase();
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
        
        /**
         * If set to false 
         * @param autoCommit
         * @return
         */
        public Builder<T, K> withAutoCommit(boolean autoCommit) {
            Preconditions.checkArgument(autoCommit == false && this.batchManager == null, "Cannot use autoCommit with an externally supplied MutationBatchManager");
            this.autoCommit = autoCommit;
            return this;
        }
        
        /**
         * If set to true log every action 
         * @param verbose
         * @return
         */
        public Builder<T, K> withVerboseTracing(boolean verbose) {
            this.verbose = verbose;
            return this;
        }
        
        /**
         * Specify a mutation manager to use.  The mutation manager makes it possible to share
         * the same mutation across multiple calls to multiple entity managers and only
         * commit when all the mutations has been created.
         * @param batchManager
         * @return
         */
        public Builder<T, K> withMutationBatchManager(MutationBatchManager batchManager) {
            this.batchManager = batchManager;
            this.autoCommit   = false;
            return this;
        }
        
        public Builder<T, K> withKeyPrefix(String prefix) {
            this.prefix = StringSerializer.get().toByteBuffer(prefix);
            return this;
        }

        @SuppressWarnings("unchecked")
        public CompositeEntityManager<T, K> build() {
            // check mandatory fields
            Preconditions.checkNotNull(clazz,    "withEntityType(...) is not set");
            Preconditions.checkNotNull(keyspace, "withKeyspace(...) is not set");
            
            // TODO: check @Id type compatibility
            // TODO: do we need to require @Entity annotation
            this.entityMapper     = new CompositeEntityMapper<T,K>(clazz, ttl, prefix);
            this.lifecycleHandler = new LifecycleEvents<T>(clazz);

            if (columnFamily == null) {
                if (columnFamilyName == null)
                    columnFamilyName = entityMapper.getEntityName();
                columnFamily = new ColumnFamily<K, ByteBuffer>(
                        columnFamilyName, 
                        (com.netflix.astyanax.Serializer<K>)MappingUtils.getSerializerForField(this.entityMapper.getId()), 
                        ByteBufferSerializer.get());
            }
            
            if (batchManager == null) {
                batchManager = new ThreadLocalMutationBatchManager(this.keyspace, this.writeConsistency, this.retryPolicy);
            }
            // build object
            return new CompositeEntityManager<T, K>(this);
        }
    }
    
    public static <T,K> Builder<T,K> builder() {
        return new Builder<T, K>();
    }
    
    private final Keyspace                    keyspace;
    private final CompositeEntityMapper<T,K>  entityMapper;
    private final RetryPolicy                 retryPolicy;
    private final LifecycleEvents<T>          lifecycleHandler;
    private final boolean                     autoCommit;
    private final ColumnFamily<K, ByteBuffer> columnFamily;
    private final ConsistencyLevel            readConsitency;
    private final MutationBatchManager        batchManager;
    private final boolean                     verbose;

    public CompositeEntityManager(Builder<T,K> builder) {
        entityMapper      = builder.entityMapper;
        keyspace          = builder.keyspace;
        columnFamily      = builder.columnFamily;
        readConsitency    = builder.readConsitency;
        retryPolicy       = builder.retryPolicy;
        lifecycleHandler  = builder.lifecycleHandler;
        autoCommit        = builder.autoCommit;
        batchManager      = builder.batchManager;
        verbose           = builder.verbose;
    }

    //////////////////////////////////////////////////////////////////
    // public APIs

    /**
     * @inheritDoc
     */
    public void put(T entity) throws PersistenceException {
        try {
            if (verbose)
                LOG.info(String.format("%s : Adding entity '%s'", columnFamily.getName(), entity));
                
            lifecycleHandler.onPrePersist(entity);
            MutationBatch mb = getMutationBatch();
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
        throw new UnsupportedOperationException("Call newNativeQuery().withId().equal({id}) instead");
    }

    /**
     * @inheritDoc
     */
    @Override
    public void delete(K id) throws PersistenceException {
        try {
            if (verbose)
                LOG.info(String.format("%s : Deleting id '%s'", columnFamily.getName(), id));
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
            if (verbose)
                LOG.info(String.format("%s : Removing entity '%s'", columnFamily.getName(), entity));
            
            lifecycleHandler.onPreRemove(entity);
            id = entityMapper.getEntityId(entity);
            MutationBatch mb = getMutationBatch();
            entityMapper.fillMutationBatchForDelete(mb, columnFamily, entity);
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
                try {
                    lifecycleHandler.onPostLoad(entity);
                } catch (Exception e) {
                    // TODO
                }
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
            if (verbose)
                LOG.info(String.format("%s : Reading entities '%s'", columnFamily.getName(), ids.toString()));
            // Query for rows
            ColumnFamilyQuery<K, ByteBuffer> cfq = newQuery();            
            return convertRowsToEntities(cfq.getRowSlice(ids).execute().getResult());
        } catch(Exception e) {
            throw new PersistenceException("failed to get entities " + ids, e);
        }
    }

    private List<T> convertRowsToEntities(Rows<K, ByteBuffer> rows) throws Exception {
        List<T> entities = Lists.newArrayList();
        for (Row<K, ByteBuffer> row : rows) {
            ColumnList<ByteBuffer> cl = row.getColumns();
            // when a row is deleted in cassandra,
            // the row key remains (without any columns) until the next compaction.
            // simply return null (as non exist)
            if (!cl.isEmpty()) {
                for (Column<ByteBuffer> column : cl) {
                    T entity = entityMapper.constructEntity(row.getKey(), column);
                    lifecycleHandler.onPostLoad(entity);
                    entities.add(entity);
                }
            }
        }
        return entities;        
    }
    
    /**
     * @inheritDoc
     */
    @Override
    public void delete(Collection<K> ids) throws PersistenceException {
        MutationBatch mb = getMutationBatch();        
        try {
            if (verbose)
                LOG.info(String.format("%s : Delete ids '%s'", columnFamily.getName(), ids.toString()));
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
                if (verbose)
                    LOG.info(String.format("%s : Deleting '%s'", columnFamily.getName(), entity));
                entityMapper.fillMutationBatchForDelete(mb, columnFamily, entity);
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
                if (verbose)
                    LOG.info(String.format("%s : Writing '%s'", columnFamily.getName(), entity));
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
            new AllRowsReader.Builder<K, ByteBuffer>(keyspace, columnFamily)
                    .withIncludeEmptyRows(false)
                    .forEachRow(new Function<Row<K,ByteBuffer>, Boolean>() {
                        @Override
                        public Boolean apply(Row<K, ByteBuffer> row) {
                            if (row.getColumns().isEmpty())
                                return true;
                            for (Column column : row.getColumns()) {
                                T entity = entityMapper.constructEntity(row.getKey(), column);
                                try {
                                    lifecycleHandler.onPostLoad(entity);
                                } catch (Exception e) {
                                    // TODO:
                                }
                                if (!callback.apply(entity))
                                    return false;
                            }
                            return true;
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
            CqlResult<K, ByteBuffer> results = newQuery().withCql(cql).execute().getResult();
            List<T> entities = Lists.newArrayListWithExpectedSize(results.getRows().size());
            for (Row<K, ByteBuffer> row : results.getRows()) {
                if (!row.getColumns().isEmpty()) { 
                    T entity = entityMapper.constructEntityFromCql(row.getColumns());
                    lifecycleHandler.onPostLoad(entity);
                    entities.add(entity);
                }
            }
            return entities;
        } catch (Exception e) {
            throw new PersistenceException("Failed to execute cql query", e);
        }
    }
    
    private MutationBatch getMutationBatch() {
        return batchManager.getSharedMutationBatch();
    }
    
    private ColumnFamilyQuery<K, ByteBuffer> newQuery() {
        ColumnFamilyQuery<K, ByteBuffer> cfq = keyspace.prepareQuery(columnFamily);
        if(readConsitency != null)
            cfq.setConsistencyLevel(readConsitency);
        if(retryPolicy != null)
            cfq.withRetryPolicy(retryPolicy);
        return cfq;
    }

    @Override
    public void createStorage(Map<String, Object> options) throws PersistenceException {
        try {
            Properties props = new Properties();
            props.put("key_validation_class",     this.entityMapper.getKeyType());
            props.put("default_validation_class", this.entityMapper.getValueType());
            props.put("comparator_type",          this.entityMapper.getComparatorType());
            props.put("name",                     this.columnFamily.getName());
            
            LOG.info("Creating column family : " + props.toString());
            keyspace.createColumnFamily(props);
        } catch (ConnectionException e) {
            throw new PersistenceException("Unable to create column family " + this.columnFamily.getName(), e);
        }
    }

    @Override
    public void deleteStorage() throws PersistenceException {
        try {
            LOG.info(String.format("%s : Deleting storage", columnFamily.getName()));
            
            keyspace.dropColumnFamily(this.columnFamily);
        } catch (ConnectionException e) {
            throw new PersistenceException("Unable to drop column family " + this.columnFamily.getName(), e);
        }
    }

    @Override
    public void truncate() throws PersistenceException {
        try {
            LOG.info(String.format("%s : Truncating", columnFamily.getName()));

            keyspace.truncateColumnFamily(this.columnFamily);
        } catch (ConnectionException e) {
            throw new PersistenceException("Unable to drop column family " + this.columnFamily.getName(), e);
        }
    }

    @Override
    public void commit() throws PersistenceException {
        if (verbose)
            LOG.info(String.format("%s : Commit mutation", columnFamily.getName()));
        
        MutationBatch mb = getMutationBatch();
        if (mb != null) {
            try {
                mb.execute();
            } catch (ConnectionException e) {
                throw new PersistenceException("Failed to commit mutation batch", e);
            }
        }
        else {
            if (verbose)
                LOG.info(String.format("%s : Nothing to commit", columnFamily.getName()));
        }
    }

    @Override
    public NativeQuery<T, K> createNativeQuery() {
        return new NativeQuery<T, K>() {
            @Override
            public T getSingleResult() throws PersistenceException {
                return Iterables.getFirst(getResultSet(), null);
            }

            @Override
            public Collection<T> getResultSet() throws PersistenceException {
                Preconditions.checkArgument(!ids.isEmpty(), "Must specify at least one row key (ID) to fetch");
                
//                if (verbose)
//                    LOG.info(String.format("%s : Query ids '%s' with predicates '%s'", columnFamily.getName(), ids, predicates));
                
                RowSliceQuery<K, ByteBuffer> rowQuery = prepareQuery();
                
                try {
                    List<T> entities = convertRowsToEntities(rowQuery.execute().getResult());
                    
//                    if (verbose)
//                        LOG.info(String.format("%s : Query ids '%s' with predicates '%s' result='%s'", columnFamily.getName(), ids, predicates, entities));
                    return entities;
                } catch (Exception e) {
                    throw new PersistenceException("Error executing query", e);
                }
            }

            @Override
            public Map<K, Collection<T>> getResultSetById() throws Exception {
                Map<K, Collection<T>> result = Maps.newLinkedHashMap();
                for (T entity : getResultSet()) {
                    K id = (K)entityMapper.idMapper.getValue(entity);
                    Collection<T> children = result.get(id);
                    if (children == null) {
                        children = Lists.newArrayListWithCapacity(1);
                        result.put(id, children);
                    }
                    children.add(entity);
                }
                return result;
            }

            @Override
            public Map<K, Integer> getResultSetCounts() throws Exception {
                Preconditions.checkArgument(!ids.isEmpty(), "Must specify at least one row key (ID) to fetch");
                
//                if (verbose)
//                    LOG.info(String.format("%s : Query ids '%s' with predicates '%s'", columnFamily.getName(), ids, predicates));
                
                RowSliceQuery<K, ByteBuffer> rowQuery = prepareQuery();
                
                try {
                    Map<K, Integer> counts = rowQuery.getColumnCounts().execute().getResult();
                    
//                    if (verbose)
//                        LOG.info(String.format("%s : Query ids '%s' with predicates '%s' result='%s'", columnFamily.getName(), ids, predicates, counts));
                    return counts;
                } catch (Exception e) {
                    throw new PersistenceException("Error executing query", e);
                }
            }
            
            private RowSliceQuery<K, ByteBuffer> prepareQuery() {
                RowSliceQuery<K, ByteBuffer> rowQuery = keyspace.prepareQuery(columnFamily).setConsistencyLevel(readConsitency)
                        .getRowSlice(ids);
                    
                if (predicates != null && !predicates.isEmpty()) {
                    ByteBuffer[] endpoints = entityMapper.getQueryEndpoints(predicates);
                    rowQuery = rowQuery.withColumnRange(
                            new RangeBuilder()
                                .setStart(endpoints[0])
                                .setEnd(endpoints[1])
                                .setLimit(columnLimit)
                                .build());
                }
                
                return rowQuery;
            }
        };
    }
}
