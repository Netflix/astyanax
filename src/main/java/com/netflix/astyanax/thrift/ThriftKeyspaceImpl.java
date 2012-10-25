/*******************************************************************************
 * Copyright 2011 Netflix
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.netflix.astyanax.thrift;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.Cassandra.Client;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.astyanax.AstyanaxConfiguration;
import com.netflix.astyanax.ColumnMutation;
import com.netflix.astyanax.Execution;
import com.netflix.astyanax.CassandraOperationType;
import com.netflix.astyanax.KeyspaceTracerFactory;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.WriteAheadEntry;
import com.netflix.astyanax.WriteAheadLog;
import com.netflix.astyanax.SerializerPackage;
import com.netflix.astyanax.connectionpool.ConnectionPool;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.TokenRange;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.connectionpool.impl.TokenRangeImpl;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.ddl.SchemaChangeResult;
import com.netflix.astyanax.ddl.impl.SchemaChangeResponseImpl;
import com.netflix.astyanax.model.*;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.retry.RetryPolicy;
import com.netflix.astyanax.retry.RunOnce;
import com.netflix.astyanax.serializers.SerializerPackageImpl;
import com.netflix.astyanax.serializers.UnknownComparatorException;
import com.netflix.astyanax.thrift.ddl.*;

public final class ThriftKeyspaceImpl implements Keyspace {

    private final ConnectionPool<Cassandra.Client> connectionPool;
    private final AstyanaxConfiguration config;
    private final String ksName;
    private final ExecutorService executor;
    private final KeyspaceTracerFactory tracerFactory;
    private final Cache<String, Object> cache;

    public ThriftKeyspaceImpl(String ksName, ConnectionPool<Cassandra.Client> pool, AstyanaxConfiguration config,
            final KeyspaceTracerFactory tracerFactory) {
        this.connectionPool = pool;
        this.config = config;
        this.ksName = ksName;
        this.executor = config.getAsyncExecutor();
        this.tracerFactory = tracerFactory;
        this.cache = CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).build();
    }

    @Override
    public String getKeyspaceName() {
        return this.ksName;
    }

    @Override
    public MutationBatch prepareMutationBatch() {
        return new AbstractThriftMutationBatchImpl(config.getClock(), config.getDefaultWriteConsistencyLevel(), config.getRetryPolicy().duplicate()) {
            @Override
            public OperationResult<Void> execute() throws ConnectionException {
                WriteAheadLog wal = getWriteAheadLog();
                WriteAheadEntry walEntry = null;
                if (wal != null) {
                    walEntry = wal.createEntry();
                    walEntry.writeMutation(this);
                }
                try {
                    OperationResult<Void> result = executeOperation(
                            new AbstractKeyspaceOperationImpl<Void>(
                                    tracerFactory.newTracer(CassandraOperationType.BATCH_MUTATE), getPinnedHost(),
                                    getKeyspaceName()) {
                                @Override
                                public Void internalExecute(Client client) throws Exception {
                                    client.batch_mutate(getMutationMap(),
                                            ThriftConverter.ToThriftConsistencyLevel(getConsistencyLevel()));
                                    discardMutations();
                                    return null;
                                }

                                @Override
                                public ByteBuffer getRowKey() {
                                    if (getMutationMap().size() == 1)
                                        return getMutationMap().keySet().iterator().next();
                                    else
                                        return null;
                                }
                            }, getRetryPolicy());

                    if (walEntry != null) {
                        wal.removeEntry(walEntry);
                    }
                    return result;
                }
                catch (ConnectionException exception) {
                    throw exception;
                }
                catch (Exception exception) {
                    throw ThriftConverter.ToConnectionPoolException(exception);
                }
            }

            @Override
            public Future<OperationResult<Void>> executeAsync() throws ConnectionException {
                return executor.submit(new Callable<OperationResult<Void>>() {
                    @Override
                    public OperationResult<Void> call() throws Exception {
                        return execute();
                    }
                });
            }
        };
    }

    @Override
    public List<TokenRange> describeRing() throws ConnectionException {
        return describeRing(null, null);
    }
    
    @Override
    public List<TokenRange> describeRing(final String dc) throws ConnectionException {
        return describeRing(dc, null);
    }
    
    @Override
    public List<TokenRange> describeRing(final String dc, final String rack) throws ConnectionException {
        return executeOperation(
                new AbstractKeyspaceOperationImpl<List<TokenRange>>(tracerFactory
                        .newTracer(CassandraOperationType.DESCRIBE_RING), getKeyspaceName()) {
                    @Override
                    public List<TokenRange> internalExecute(Cassandra.Client client) throws Exception {
                        List<org.apache.cassandra.thrift.TokenRange> trs = client.describe_ring(getKeyspaceName());
                        List<TokenRange> range = Lists.newArrayList();
                        
                        for (org.apache.cassandra.thrift.TokenRange tr : trs) {
                            List<String> endpoints = Lists.newArrayList();
                            for (org.apache.cassandra.thrift.EndpointDetails ed : tr.getEndpoint_details()) {
                                if (dc != null && !ed.getDatacenter().equals(dc)) {
                                    continue;
                                }
                                else if (rack != null && !ed.getRack().equals(dc)) {
                                    continue;
                                }
                                else {
                                    endpoints.add(ed.getHost());
                                }
                            }
                            
                            if (!endpoints.isEmpty()) {
                                range.add(new TokenRangeImpl(tr.getStart_token(), tr.getEnd_token(), endpoints));
                            }
                        }
                        return range;
                    }
                }, getConfig().getRetryPolicy().duplicate()).getResult();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<TokenRange> describeRing(boolean cached) throws ConnectionException {
        if (cached) {
          try {
            return (List<TokenRange>) this.cache.get(CassandraOperationType.DESCRIBE_RING.name(),
                  new Callable<Object>() {
                      @Override
                      public Object call() throws Exception {
                          return describeRing();
                      }
                  });
            }
            catch (ExecutionException e) {
                throw ThriftConverter.ToConnectionPoolException(e);
            }
        }
        else {
            return describeRing();
        }
    }
    
    @Override
    public KeyspaceDefinition describeKeyspace() throws ConnectionException {
        return executeOperation(
                new AbstractKeyspaceOperationImpl<KeyspaceDefinition>(
                        tracerFactory.newTracer(CassandraOperationType.DESCRIBE_KEYSPACE), getKeyspaceName()) {
                    @Override
                    public KeyspaceDefinition internalExecute(Cassandra.Client client) throws Exception {
                        return new ThriftKeyspaceDefinitionImpl(client.describe_keyspace(getKeyspaceName()));
                    }
                }, getConfig().getRetryPolicy().duplicate()).getResult();
    }
    
    public <K, C> ColumnFamilyQuery<K, C> prepareQuery(ColumnFamily<K, C> cf) {
        return new ThriftColumnFamilyQueryImpl<K, C>(executor, tracerFactory, this, connectionPool, cf,
                config.getDefaultReadConsistencyLevel(), config.getRetryPolicy());
    }

    @Override
    public <K, C> ColumnMutation prepareColumnMutation(final ColumnFamily<K, C> columnFamily, final K rowKey, C column) {
        return new AbstractThriftColumnMutationImpl(columnFamily.getKeySerializer().toByteBuffer(rowKey), columnFamily
                .getColumnSerializer().toByteBuffer(column), config.getClock()) {

            private RetryPolicy retry = config.getRetryPolicy().duplicate();
            private ConsistencyLevel writeConsistencyLevel = config.getDefaultWriteConsistencyLevel();

            @Override
            public ColumnMutation setConsistencyLevel(ConsistencyLevel consistencyLevel) {
                writeConsistencyLevel = consistencyLevel;
                return this;
            }

            @Override
            public Execution<Void> incrementCounterColumn(final long amount) {
                return new Execution<Void>() {
                    @Override
                    public OperationResult<Void> execute() throws ConnectionException {
                        return executeOperation(
                                new AbstractKeyspaceOperationImpl<Void>(
                                        tracerFactory.newTracer(CassandraOperationType.COUNTER_MUTATE),
                                        getKeyspaceName()) {
                                    @Override
                                    public Void internalExecute(Client client) throws Exception {
                                        client.add(key, ThriftConverter.getColumnParent(columnFamily, null),
                                                new CounterColumn().setValue(amount).setName(column),
                                                ThriftConverter.ToThriftConsistencyLevel(writeConsistencyLevel));
                                        return null;
                                    }

                                    @Override
                                    public ByteBuffer getRowKey() {
                                        return columnFamily.getKeySerializer()
                                                .toByteBuffer(rowKey);
                                    }
                                }, retry);
                    }

                    @Override
                    public Future<OperationResult<Void>> executeAsync() throws ConnectionException {
                        return executor.submit(new Callable<OperationResult<Void>>() {
                            @Override
                            public OperationResult<Void> call() throws Exception {
                                return execute();
                            }
                        });
                    }
                };
            }

            @Override
            public Execution<Void> deleteColumn() {
                return new Execution<Void>() {
                    @Override
                    public OperationResult<Void> execute() throws ConnectionException {
                        return executeOperation(
                                new AbstractKeyspaceOperationImpl<Void>(
                                        tracerFactory.newTracer(CassandraOperationType.COLUMN_DELETE),
                                        getKeyspaceName()) {
                                    @Override
                                    public Void internalExecute(Client client) throws Exception {
                                        client.remove(key, new org.apache.cassandra.thrift.ColumnPath()
                                                .setColumn_family(columnFamily.getName()).setColumn(column), config
                                                .getClock().getCurrentTime(), ThriftConverter
                                                .ToThriftConsistencyLevel(writeConsistencyLevel));
                                        return null;
                                    }

                                    @Override
                                    public ByteBuffer getRowKey() {
                                        return columnFamily.getKeySerializer().toByteBuffer(rowKey);
                                    }
                                }, retry);
                    }

                    @Override
                    public Future<OperationResult<Void>> executeAsync() throws ConnectionException {
                        return executor.submit(new Callable<OperationResult<Void>>() {
                            @Override
                            public OperationResult<Void> call() throws Exception {
                                return execute();
                            }
                        });
                    }
                };
            }

            @Override
            public Execution<Void> insertValue(final ByteBuffer value, final Integer ttl) {
                return new Execution<Void>() {
                    @Override
                    public OperationResult<Void> execute() throws ConnectionException {
                        return executeOperation(
                                new AbstractKeyspaceOperationImpl<Void>(
                                        tracerFactory.newTracer(CassandraOperationType.COLUMN_INSERT),
                                        getKeyspaceName()) {
                                    @Override
                                    public Void internalExecute(Client client) throws Exception {
                                        org.apache.cassandra.thrift.Column c = new org.apache.cassandra.thrift.Column();
                                        c.setName(column).setValue(value).setTimestamp(clock.getCurrentTime());
                                        if (ttl != null) {
                                            c.setTtl(ttl);
                                        }

                                        client.insert(key, ThriftConverter.getColumnParent(columnFamily, null), c,
                                                ThriftConverter.ToThriftConsistencyLevel(writeConsistencyLevel));
                                        return null;
                                    }

                                    @Override
                                    public ByteBuffer getRowKey() {
                                        return columnFamily.getKeySerializer().toByteBuffer(rowKey);
                                    }
                                }, retry);
                    }

                    @Override
                    public Future<OperationResult<Void>> executeAsync() throws ConnectionException {
                        return executor.submit(new Callable<OperationResult<Void>>() {
                            @Override
                            public OperationResult<Void> call() throws Exception {
                                return execute();
                            }
                        });
                    }
                };
            }

            @Override
            public Execution<Void> deleteCounterColumn() {
                return new Execution<Void>() {
                    @Override
                    public OperationResult<Void> execute() throws ConnectionException {
                        return executeOperation(
                                new AbstractKeyspaceOperationImpl<Void>(
                                        tracerFactory.newTracer(CassandraOperationType.COLUMN_DELETE),
                                        getKeyspaceName()) {
                                    @Override
                                    public Void internalExecute(Client client) throws Exception {
                                        client.remove_counter(key, new org.apache.cassandra.thrift.ColumnPath()
                                                .setColumn_family(columnFamily.getName()).setColumn(column),
                                                ThriftConverter.ToThriftConsistencyLevel(writeConsistencyLevel));
                                        return null;
                                    }

                                    @Override
                                    public ByteBuffer getRowKey() {
                                        return columnFamily.getKeySerializer().toByteBuffer(rowKey);
                                    }
                                }, retry);
                    }

                    @Override
                    public Future<OperationResult<Void>> executeAsync() throws ConnectionException {
                        return executor.submit(new Callable<OperationResult<Void>>() {
                            @Override
                            public OperationResult<Void> call() throws Exception {
                                return execute();
                            }
                        });
                    }
                };
            }

            @Override
            public ColumnMutation withRetryPolicy(RetryPolicy retry) {
                this.retry = retry;
                return this;
            }
        };
    }

    @Override
    public AstyanaxConfiguration getConfig() {
        return this.config;
    }

    @Override
    public SerializerPackage getSerializerPackage(String cfName, boolean ignoreErrors) throws ConnectionException,
            UnknownComparatorException {
        return new SerializerPackageImpl(describeKeyspace().getColumnFamily(cfName), ignoreErrors);
    }

    @Override
    public OperationResult<Void> testOperation(final Operation<?, ?> operation) throws ConnectionException {
        return testOperation(operation, config.getRetryPolicy().duplicate());
    }

    @Override
    public OperationResult<Void> testOperation(final Operation<?, ?> operation, RetryPolicy retry)
            throws ConnectionException {
        return executeOperation(
                new AbstractKeyspaceOperationImpl<Void>(tracerFactory.newTracer(CassandraOperationType.TEST),
                        operation.getPinnedHost(), getKeyspaceName()) {
                    @Override
                    public Void internalExecute(Client client) throws Exception {
                        operation.execute(null);
                        return null;
                    }
                }, retry);
    }

    ConnectionPool<Cassandra.Client> getConnectionPool() {
        return connectionPool;
    }

    @Override
    public <K, C> OperationResult<Void> truncateColumnFamily(final ColumnFamily<K, C> columnFamily)
            throws OperationException, ConnectionException {
        return truncateColumnFamily(columnFamily.getName());
    }

    @Override
    public OperationResult<Void> truncateColumnFamily(final String columnFamily) throws ConnectionException {
        return executeOperation(
                new AbstractKeyspaceOperationImpl<Void>(tracerFactory.newTracer(CassandraOperationType.TRUNCATE),
                        getKeyspaceName()) {
                    @Override
                    public Void internalExecute(Cassandra.Client client) throws Exception {
                        client.truncate(columnFamily);
                        return null;
                    }
                }, config.getRetryPolicy().duplicate());
    }

    private <R> OperationResult<R> executeOperation(Operation<Cassandra.Client, R> operation, RetryPolicy retry)
            throws OperationException, ConnectionException {
        return connectionPool.executeWithFailover(operation, retry);
    }

    @Override
    public String describePartitioner() throws ConnectionException {
        return connectionPool
                .executeWithFailover(
                        new AbstractOperationImpl<String>(
                                tracerFactory.newTracer(CassandraOperationType.DESCRIBE_PARTITIONER)) {
                            @Override
                            public String internalExecute(Client client) throws Exception {
                                return client.describe_partitioner();
                            }
                        }, config.getRetryPolicy().duplicate()).getResult();
    }

    @Override
    public <K, C> OperationResult<SchemaChangeResult> createColumnFamily(final ColumnFamily<K, C> columnFamily, final Map<String, Object> options) throws ConnectionException {
        return connectionPool
                .executeWithFailover(
                        new AbstractKeyspaceOperationImpl<SchemaChangeResult>(
                                tracerFactory.newTracer(CassandraOperationType.ADD_COLUMN_FAMILY), getKeyspaceName()) {
                            @Override
                            public SchemaChangeResult internalExecute(Client client) throws Exception {
                                ThriftColumnFamilyDefinitionImpl def = new ThriftColumnFamilyDefinitionImpl();
                                
                                Map<String, Object> internalOptions = Maps.newHashMap();
                                if (options != null)
                                    internalOptions.putAll(options);
                                
                                internalOptions.put("name",                 columnFamily.getName());
                                internalOptions.put("keyspace",             getKeyspaceName());
                                if (!internalOptions.containsKey("comparator_type"))
                                    internalOptions.put("comparator_type",      columnFamily.getColumnSerializer().getComparatorType().getTypeName());
                                if (!internalOptions.containsKey("key_validation_class"))
                                    internalOptions.put("key_validation_class", columnFamily.getKeySerializer().getComparatorType().getTypeName());
                                
                                def.setFields(internalOptions);
                                
                                return new SchemaChangeResponseImpl()
                                    .setSchemaId(client.system_add_column_family(def.getThriftColumnFamilyDefinition()));
                            }
                        }, RunOnce.get());
    }
    
    @Override
    public <K, C> OperationResult<SchemaChangeResult> updateColumnFamily(final ColumnFamily<K, C> columnFamily, final Map<String, Object> options) throws ConnectionException  {
        return connectionPool
                .executeWithFailover(
                        new AbstractKeyspaceOperationImpl<SchemaChangeResult>(
                                tracerFactory.newTracer(CassandraOperationType.UPDATE_COLUMN_FAMILY), getKeyspaceName()) {
                            @Override
                            public SchemaChangeResult internalExecute(Client client) throws Exception {
                                ThriftColumnFamilyDefinitionImpl def = new ThriftColumnFamilyDefinitionImpl();
                                
                                Map<String, Object> internalOptions = Maps.newHashMap();
                                if (options != null)
                                    internalOptions.putAll(options);
                                internalOptions.put("name",                 columnFamily.getName());
                                internalOptions.put("keyspace",             getKeyspaceName());
                                if (!internalOptions.containsKey("key_validation_class"))
                                    internalOptions.put("key_validation_class", columnFamily.getKeySerializer().getComparatorType().getTypeName());
                                
                                def.setFields(internalOptions);
                                
                                return new SchemaChangeResponseImpl()
                                    .setSchemaId(client.system_update_column_family(def.getThriftColumnFamilyDefinition()));
                            }
                        }, RunOnce.get());
    }

    @Override
    public OperationResult<SchemaChangeResult> dropColumnFamily(final String columnFamilyName) throws ConnectionException  {
        return connectionPool
                .executeWithFailover(
                        new AbstractKeyspaceOperationImpl<SchemaChangeResult>(
                                tracerFactory.newTracer(CassandraOperationType.DROP_COLUMN_FAMILY), getKeyspaceName()) {
                            @Override
                            public SchemaChangeResult internalExecute(Client client) throws Exception {
                                return new SchemaChangeResponseImpl()
                                    .setSchemaId(client.system_drop_column_family(columnFamilyName));
                            }
                        }, RunOnce.get());
    }

    @Override
    public <K, C> OperationResult<SchemaChangeResult> dropColumnFamily(final ColumnFamily<K, C> columnFamily) throws ConnectionException  {
        return connectionPool
                .executeWithFailover(
                        new AbstractKeyspaceOperationImpl<SchemaChangeResult>(
                                tracerFactory.newTracer(CassandraOperationType.DROP_COLUMN_FAMILY), getKeyspaceName()) {
                            @Override
                            public SchemaChangeResult internalExecute(Client client) throws Exception {
                                return new SchemaChangeResponseImpl()
                                    .setSchemaId(client.system_drop_column_family(columnFamily.getName()));
                            }
                        }, RunOnce.get());
    }

    @Override
    public OperationResult<SchemaChangeResult> createKeyspace(final Map<String, Object> options) throws ConnectionException  {
        return connectionPool
                .executeWithFailover(
                        new AbstractOperationImpl<SchemaChangeResult>(
                                tracerFactory.newTracer(CassandraOperationType.ADD_KEYSPACE)) {
                            @Override
                            public SchemaChangeResult internalExecute(Client client) throws Exception {
                                ThriftKeyspaceDefinitionImpl def = new ThriftKeyspaceDefinitionImpl();
                                
                                Map<String, Object> internalOptions = Maps.newHashMap();
                                if (options != null)
                                    internalOptions.putAll(options);
                                internalOptions.put("name",                 getKeyspaceName());
                                
                                def.setFields(internalOptions);
                                
                                return new SchemaChangeResponseImpl()
                                    .setSchemaId(client.system_add_keyspace(def.getThriftKeyspaceDefinition()));
                            }
                        }, RunOnce.get());
    }


    @Override
    public OperationResult<SchemaChangeResult> updateKeyspace(final Map<String, Object> options) throws ConnectionException  {
        return connectionPool
                .executeWithFailover(
                        new AbstractKeyspaceOperationImpl<SchemaChangeResult>(
                                tracerFactory.newTracer(CassandraOperationType.UPDATE_KEYSPACE), getKeyspaceName()) {
                            @Override
                            public SchemaChangeResult internalExecute(Client client) throws Exception {
                                ThriftKeyspaceDefinitionImpl def = new ThriftKeyspaceDefinitionImpl();
                                
                                Map<String, Object> internalOptions = Maps.newHashMap();
                                if (options != null)
                                    internalOptions.putAll(options);
                                internalOptions.put("name",                 getKeyspaceName());
                                
                                def.setFields(internalOptions);
                                
                                return new SchemaChangeResponseImpl()
                                    .setSchemaId(client.system_update_keyspace(def.getThriftKeyspaceDefinition()));
                            }
                        }, RunOnce.get());
    }

    @Override
    public OperationResult<SchemaChangeResult> dropKeyspace() throws ConnectionException  {
        return connectionPool
                .executeWithFailover(
                        new AbstractKeyspaceOperationImpl<SchemaChangeResult>(
                                tracerFactory.newTracer(CassandraOperationType.DROP_KEYSPACE), getKeyspaceName()) {
                            @Override
                            public SchemaChangeResult internalExecute(Client client) throws Exception {
                                return new SchemaChangeResponseImpl()
                                    .setSchemaId(client.system_drop_keyspace(getKeyspaceName()));
                            }
                        }, RunOnce.get());
    }
}
