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
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
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
import com.netflix.astyanax.connectionpool.ConnectionContext;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.TokenRange;
import com.netflix.astyanax.connectionpool.exceptions.BadRequestException;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.IsDeadConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.connectionpool.exceptions.SchemaDisagreementException;
import com.netflix.astyanax.connectionpool.impl.TokenRangeImpl;
import com.netflix.astyanax.cql.CqlStatement;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.ddl.SchemaChangeResult;
import com.netflix.astyanax.ddl.impl.SchemaChangeResponseImpl;
import com.netflix.astyanax.model.*;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.partitioner.Partitioner;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.retry.RetryPolicy;
import com.netflix.astyanax.retry.RunOnce;
import com.netflix.astyanax.serializers.SerializerPackageImpl;
import com.netflix.astyanax.serializers.UnknownComparatorException;
import com.netflix.astyanax.thrift.ddl.*;

public final class ThriftKeyspaceImpl implements Keyspace {
    private final static Logger LOG = LoggerFactory.getLogger(ThriftKeyspaceImpl.class);
    
    final ConnectionPool<Cassandra.Client> connectionPool;
    final AstyanaxConfiguration config;
    final String                ksName;
    final ListeningExecutorService executor;
    final KeyspaceTracerFactory tracerFactory;
    final Cache<String, Object> cache;
    final ThriftCqlFactory      cqlStatementFactory;
    private Host                  ddlHost = null;
    private volatile Partitioner  partitioner;
    
    public ThriftKeyspaceImpl(
            String ksName, 
            ConnectionPool<Cassandra.Client> pool, 
            AstyanaxConfiguration config,
            final KeyspaceTracerFactory tracerFactory) {
        this.connectionPool = pool;
        this.config         = config;
        this.ksName         = ksName;
        this.executor       = MoreExecutors.listeningDecorator(config.getAsyncExecutor());
        this.tracerFactory  = tracerFactory;
        this.cache          = CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).build();
        this.cqlStatementFactory = ThriftCqlFactoryResolver.createFactory(config);
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
                                    tracerFactory.newTracer(useAtomicBatch() ? CassandraOperationType.ATOMIC_BATCH_MUTATE : CassandraOperationType.BATCH_MUTATE), 
                                                            getPinnedHost(),
                                                            getKeyspaceName()) {
                                @Override
                                public Void internalExecute(Client client, ConnectionContext context) throws Exception {
                                    // Mutation can be atomic or non-atomic. 
                                    // see http://www.datastax.com/dev/blog/atomic-batches-in-cassandra-1-2 for details on atomic batches
                                    if (useAtomicBatch()) {
                                        client.atomic_batch_mutate(getMutationMap(),
                                                ThriftConverter.ToThriftConsistencyLevel(getConsistencyLevel()));
                                    } else {
                                        client.batch_mutate(getMutationMap(),
                                                ThriftConverter.ToThriftConsistencyLevel(getConsistencyLevel()));
                                    }
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
            public ListenableFuture<OperationResult<Void>> executeAsync() throws ConnectionException {
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
                    public List<TokenRange> internalExecute(Cassandra.Client client, ConnectionContext context) throws Exception {
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
                    public KeyspaceDefinition internalExecute(Cassandra.Client client, ConnectionContext context) throws Exception {
                        return new ThriftKeyspaceDefinitionImpl(client.describe_keyspace(getKeyspaceName()));
                    }
                }, getConfig().getRetryPolicy().duplicate()).getResult();
    }

    @Override
    public Map<String, List<String>> describeSchemaVersions() throws ConnectionException {
        return connectionPool.executeWithFailover(
                new AbstractOperationImpl<Map<String, List<String>>>(
                        tracerFactory.newTracer(CassandraOperationType.DESCRIBE_SCHEMA_VERSION)) {
                    @Override
                    public Map<String, List<String>> internalExecute(Client client, ConnectionContext context) throws Exception {
                        return client.describe_schema_versions();
                    }
                }, config.getRetryPolicy().duplicate()).getResult();
    }

    public <K, C> ColumnFamilyQuery<K, C> prepareQuery(ColumnFamily<K, C> cf) {
        return new ThriftColumnFamilyQueryImpl<K, C>(
                executor,
                tracerFactory,
                this,
                connectionPool,
                cf,
                config.getDefaultReadConsistencyLevel(),
                config.getRetryPolicy());
    }

    @Override
    public <K, C> ColumnMutation prepareColumnMutation(final ColumnFamily<K, C> columnFamily, final K rowKey, C column) {
        return new AbstractThriftColumnMutationImpl(
                columnFamily.getKeySerializer().toByteBuffer(rowKey),
                columnFamily.getColumnSerializer().toByteBuffer(column),
                config) {

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
                                    public Void internalExecute(Client client, ConnectionContext context) throws Exception {
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
                    public ListenableFuture<OperationResult<Void>> executeAsync() throws ConnectionException {
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
                                    public Void internalExecute(Client client, ConnectionContext context) throws Exception {
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
                    public ListenableFuture<OperationResult<Void>> executeAsync() throws ConnectionException {
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
                                    public Void internalExecute(Client client, ConnectionContext context) throws Exception {
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
                    public ListenableFuture<OperationResult<Void>> executeAsync() throws ConnectionException {
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
                                    public Void internalExecute(Client client, ConnectionContext context) throws Exception {
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
                    public ListenableFuture<OperationResult<Void>> executeAsync() throws ConnectionException {
                        return executor.submit(new Callable<OperationResult<Void>>() {
                            @Override
                            public OperationResult<Void> call() throws Exception {
                                return execute();
                            }
                        });
                    }
                };
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
                    public Void internalExecute(Client client, ConnectionContext context) throws Exception {
                        operation.execute(null, context);
                        return null;
                    }
                }, retry);
    }

    @Override
    public ConnectionPool<Cassandra.Client> getConnectionPool() {
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
                    public Void internalExecute(Cassandra.Client client, ConnectionContext context) throws Exception {
                        client.truncate(columnFamily);
                        return null;
                    }
                }, config.getRetryPolicy().duplicate());
    }

    private <R> OperationResult<R> executeOperation(Operation<Cassandra.Client, R> operation, RetryPolicy retry)
            throws OperationException, ConnectionException {
        return connectionPool.executeWithFailover(operation, retry);
    }

        
    /**
     * Attempt to execute the DDL operation on the same host
     * @param operation
     * @param retry
     * @return
     * @throws OperationException
     * @throws ConnectionException
     */
    private synchronized <R> OperationResult<R> executeDdlOperation(AbstractOperationImpl<R> operation, RetryPolicy retry)
             throws OperationException, ConnectionException {
         ConnectionException lastException = null;
         for (int i = 0; i < 2; i++) {
             operation.setPinnedHost(ddlHost);
             try {
                 OperationResult<R> result = connectionPool.executeWithFailover(operation, retry);
                 ddlHost = result.getHost();
                 return result;
             }
             catch (ConnectionException e) {
                lastException = e;
                if (e instanceof IsDeadConnectionException) {
                     ddlHost = null;
                }
             }
         }
         throw lastException;
    }

    @Override
    public String describePartitioner() throws ConnectionException {
        return executeOperation(
                        new AbstractOperationImpl<String>(
                                tracerFactory.newTracer(CassandraOperationType.DESCRIBE_PARTITIONER)) {
                            @Override
                            public String internalExecute(Client client, ConnectionContext context) throws Exception {
                                return client.describe_partitioner();
                            }
                        }, config.getRetryPolicy().duplicate()).getResult();
    }

    @Override
    public OperationResult<SchemaChangeResult> createColumnFamily(final Map<String, Object> options) throws ConnectionException {
        final CfDef cfDef = toThriftColumnFamilyDefinition(options, null).getThriftColumnFamilyDefinition();
        return internalCreateColumnFamily(cfDef);
    }

    @Override
    public OperationResult<SchemaChangeResult> createKeyspace(
            final Map<String, Object> options,
            final Map<ColumnFamily, Map<String, Object>> cfs) throws ConnectionException {
        
        ThriftKeyspaceDefinitionImpl ksDef = toThriftKeyspaceDefinition(options);
        for (Entry<ColumnFamily, Map<String, Object>> cf : cfs.entrySet()) {
            ksDef.addColumnFamily(toThriftColumnFamilyDefinition(cf.getValue(), cf.getKey()));
        }
        
        ksDef.setName(getKeyspaceName());

        return internalCreateKeyspace(ksDef.getThriftKeyspaceDefinition());
    }    
    
    @Override
    public OperationResult<SchemaChangeResult> createKeyspace(final Properties props) throws ConnectionException {
        if (props.containsKey("name") && !props.get("name").equals(getKeyspaceName())) { 
            throw new BadRequestException(
                    String.format("'name' attribute must match keyspace name. Expected '%s' but got '%s'", 
                                  getKeyspaceName(), props.get("name")));
        }
        
        final KsDef ksDef;
        try {
            ksDef = ThriftUtils.getThriftObjectFromProperties(KsDef.class, props);
        } catch (Exception e) {
            throw new BadRequestException("Unable to convert props to keyspace definition");
        }
        
        return internalCreateKeyspace(ksDef);
    }


    @Override
    public <K, C> OperationResult<SchemaChangeResult> createColumnFamily(final ColumnFamily<K, C> columnFamily, final Map<String, Object> options) throws ConnectionException {
        final CfDef cfDef = toThriftColumnFamilyDefinition(options, columnFamily).getThriftColumnFamilyDefinition();
        return internalCreateColumnFamily(cfDef);
    }

    @Override
    public <K, C> OperationResult<SchemaChangeResult> updateColumnFamily(final ColumnFamily<K, C> columnFamily, final Map<String, Object> options) throws ConnectionException  {
        final CfDef cfDef = toThriftColumnFamilyDefinition(options, columnFamily).getThriftColumnFamilyDefinition();
        return internalUpdateColumnFamily(cfDef);
    }

    @Override
    public OperationResult<SchemaChangeResult> dropColumnFamily(final String columnFamilyName) throws ConnectionException  {
        return executeDdlOperation(
                        new AbstractKeyspaceOperationImpl<SchemaChangeResult>(
                                tracerFactory.newTracer(CassandraOperationType.DROP_COLUMN_FAMILY), getKeyspaceName()) {
                            @Override
                            public SchemaChangeResult internalExecute(Client client, ConnectionContext context) throws Exception {
                                precheckSchemaAgreement(client);
                                return new SchemaChangeResponseImpl()
                                    .setSchemaId(client.system_drop_column_family(columnFamilyName));
                            }
                        }, RunOnce.get());
    }

    @Override
    public <K, C> OperationResult<SchemaChangeResult> dropColumnFamily(final ColumnFamily<K, C> columnFamily) throws ConnectionException  {
        return dropColumnFamily(columnFamily.getName());
    }

    @Override
    public OperationResult<SchemaChangeResult> createKeyspace(final Map<String, Object> options) throws ConnectionException  {
        final KsDef ksDef = toThriftKeyspaceDefinition(options).getThriftKeyspaceDefinition();
        return internalCreateKeyspace(ksDef);
    }


    @Override
    public OperationResult<SchemaChangeResult> updateKeyspace(final Map<String, Object> options) throws ConnectionException  {
        final KsDef ksDef = toThriftKeyspaceDefinition(options).getThriftKeyspaceDefinition();
        return internalUpdateKeyspace(ksDef);
    }

    @Override
    public OperationResult<SchemaChangeResult> dropKeyspace() throws ConnectionException  {
        return executeDdlOperation(
                        new AbstractKeyspaceOperationImpl<SchemaChangeResult>(
                                tracerFactory.newTracer(CassandraOperationType.DROP_KEYSPACE), getKeyspaceName()) {
                            @Override
                            public SchemaChangeResult internalExecute(Client client, ConnectionContext context) throws Exception {
                                precheckSchemaAgreement(client);
                                return new SchemaChangeResponseImpl()
                                    .setSchemaId(client.system_drop_keyspace(getKeyspaceName()));
                            }
                        }, RunOnce.get());
    }

    @Override
    public CqlStatement prepareCqlStatement() {
        return this.cqlStatementFactory.createCqlStatement(this);
    }

    @Override
    public Partitioner getPartitioner() throws ConnectionException {
        if (partitioner == null) {
            synchronized(this) {
                if (partitioner == null) {
                    String partitionerName = this.describePartitioner();
                    try {
                        partitioner = config.getPartitioner(partitionerName);
                        LOG.info(String.format("Detected partitioner %s for keyspace %s", partitionerName, ksName));
                    } catch (Exception e) {
                        throw new NotFoundException("Unable to determine partitioner " + partitionerName, e);
                    }
                }
            }
        }
        return partitioner;
    }
    
    /**
     * Do a quick check to see if there is a schema disagreement.  This is done as an extra precaution
     * to reduce the chances of putting the cluster into a bad state.  This will not gurantee however, that 
     * by the time a schema change is made the cluster will be in the same state.
     * @param client
     * @throws Exception
     */
    private void precheckSchemaAgreement(Client client) throws Exception {
        Map<String, List<String>> schemas = client.describe_schema_versions();
        if (schemas.size() > 1) {
            throw new SchemaDisagreementException("Can't change schema due to pending schema agreement");
        }
    }
    
    /**
     * Convert a Map of options to an internal thrift column family definition
     * @param options
     */
    private ThriftColumnFamilyDefinitionImpl toThriftColumnFamilyDefinition(Map<String, Object> options, ColumnFamily columnFamily) {
        ThriftColumnFamilyDefinitionImpl def = new ThriftColumnFamilyDefinitionImpl();

        Map<String, Object> internalOptions = Maps.newHashMap();
        if (options != null)
            internalOptions.putAll(options);

        internalOptions.put("keyspace", getKeyspaceName());
        
        if (columnFamily != null) {
            internalOptions.put("name", columnFamily.getName());
            if (!internalOptions.containsKey("comparator_type"))
                internalOptions.put("comparator_type", columnFamily.getColumnSerializer().getComparatorType().getTypeName());
            if (!internalOptions.containsKey("key_validation_class"))
                internalOptions.put("key_validation_class", columnFamily.getKeySerializer().getComparatorType().getTypeName());
            if (columnFamily.getDefaultValueSerializer() != null && !internalOptions.containsKey("default_validation_class"))
                internalOptions.put("default_validation_class", columnFamily.getDefaultValueSerializer().getComparatorType().getTypeName());
        }

        def.setFields(internalOptions);
        return def;
    }
    
    /**
     * Convert a Map of options to an internal thrift keyspace definition
     * @param options
     */
    private ThriftKeyspaceDefinitionImpl toThriftKeyspaceDefinition(final Map<String, Object> options) {
        ThriftKeyspaceDefinitionImpl def = new ThriftKeyspaceDefinitionImpl();
        
        Map<String, Object> internalOptions = Maps.newHashMap();
        if (options != null)
            internalOptions.putAll(options);
        
        if (options.containsKey("name") && !options.get("name").equals(getKeyspaceName())) {
            throw new RuntimeException(
                    String.format("'name' attribute must match keyspace name. Expected '%s' but got '%s'", 
                                  getKeyspaceName(), options.get("name")));
        }
        else {
            internalOptions.put("name", getKeyspaceName());
        }
        
        def.setFields(internalOptions); 
        
        return def;
    }

    @Override
    public OperationResult<SchemaChangeResult> updateKeyspace(final Properties props) throws ConnectionException {
        if (props.containsKey("name") && props.get("name").equals(getKeyspaceName())) { 
            throw new RuntimeException(
                    String.format("'name' attribute must match keyspace name. Expected '%s' but got '%s'", 
                                  getKeyspaceName(), props.get("name")));
        }
        
        final KsDef ksDef;
        try {
            ksDef = ThriftUtils.getThriftObjectFromProperties(KsDef.class, props);
        } catch (Exception e) {
            throw new BadRequestException("Unable to convert properties to KsDef", e);
        }
        ksDef.setName(getKeyspaceName());
        
        return internalUpdateKeyspace(ksDef);
    }

    public OperationResult<SchemaChangeResult> internalUpdateKeyspace(final KsDef ksDef) throws ConnectionException {
        return connectionPool
                .executeWithFailover(
                        new AbstractOperationImpl<SchemaChangeResult>(
                                tracerFactory.newTracer(CassandraOperationType.UPDATE_KEYSPACE)) {
                            @Override
                            public SchemaChangeResult internalExecute(Client client, ConnectionContext context) throws Exception {
                                precheckSchemaAgreement(client);
                                return new SchemaChangeResponseImpl().setSchemaId(client.system_update_keyspace(ksDef));
                            }
                        }, RunOnce.get());
    }

    public OperationResult<SchemaChangeResult> internalCreateKeyspace(final KsDef ksDef) throws ConnectionException {
        if (ksDef.getCf_defs() == null)
            ksDef.setCf_defs(Lists.<CfDef>newArrayList());
        
        return executeDdlOperation(
                        new AbstractOperationImpl<SchemaChangeResult>(
                                tracerFactory.newTracer(CassandraOperationType.ADD_KEYSPACE)) {
                            @Override
                            public SchemaChangeResult internalExecute(Client client, ConnectionContext context) throws Exception {
                                precheckSchemaAgreement(client);
                                return new SchemaChangeResponseImpl().setSchemaId(client.system_add_keyspace(ksDef));
                            }
                        }, RunOnce.get());
    }

    @Override
    public OperationResult<SchemaChangeResult> createColumnFamily(final Properties props) throws ConnectionException {
        if (props.containsKey("keyspace") && props.get("keyspace").equals(getKeyspaceName())) { 
            throw new RuntimeException(
                    String.format("'keyspace' attribute must match keyspace name. Expected '%s' but got '%s'", 
                                  getKeyspaceName(), props.get("keyspace")));
        }
        
        CfDef cfDef;
        try {
            cfDef = ThriftUtils.getThriftObjectFromProperties(CfDef.class, props);
        } catch (Exception e) {
            throw new BadRequestException("Unable to convert properties to CfDef", e);
        }
        cfDef.setKeyspace(getKeyspaceName());
        return internalCreateColumnFamily(cfDef);
    }
    
    private OperationResult<SchemaChangeResult> internalCreateColumnFamily(final CfDef cfDef) throws ConnectionException {
        return executeDdlOperation(new AbstractKeyspaceOperationImpl<SchemaChangeResult>(
                                tracerFactory.newTracer(CassandraOperationType.ADD_COLUMN_FAMILY), getKeyspaceName()) {
                            @Override
                            public SchemaChangeResult internalExecute(Client client, ConnectionContext context) throws Exception {
                                precheckSchemaAgreement(client);
                                LOG.info(cfDef.toString());
                                return new SchemaChangeResponseImpl().setSchemaId(client.system_add_column_family(cfDef));
                            }
                        }, RunOnce.get());
    }
    
    private OperationResult<SchemaChangeResult> internalUpdateColumnFamily(final CfDef cfDef) throws ConnectionException {
        return executeDdlOperation(
                        new AbstractKeyspaceOperationImpl<SchemaChangeResult>(
                                tracerFactory.newTracer(CassandraOperationType.ADD_COLUMN_FAMILY), getKeyspaceName()) {
                            @Override
                            public SchemaChangeResult internalExecute(Client client, ConnectionContext context) throws Exception {
                                precheckSchemaAgreement(client);
                                return new SchemaChangeResponseImpl().setSchemaId(client.system_update_column_family(cfDef));
                            }
                        }, RunOnce.get());
    }

    @Override
    public OperationResult<SchemaChangeResult> updateColumnFamily(final Map<String, Object> options) throws ConnectionException  {
        if (options.containsKey("keyspace") && options.get("keyspace").equals(getKeyspaceName())) { 
            throw new RuntimeException(
                    String.format("'keyspace' attribute must match keyspace name. Expected '%s' but got '%s'", 
                                  getKeyspaceName(), options.get("name")));
        }
        
        return connectionPool
                .executeWithFailover(
                        new AbstractKeyspaceOperationImpl<SchemaChangeResult>(
                                tracerFactory.newTracer(CassandraOperationType.UPDATE_COLUMN_FAMILY), (String)options.get("keyspace")) {
                            @Override
                            public SchemaChangeResult internalExecute(Client client, ConnectionContext context) throws Exception {
                                ThriftColumnFamilyDefinitionImpl def = new ThriftColumnFamilyDefinitionImpl();
                                def.setFields(options);
                                def.setKeyspace(getKeyspaceName());
                                
                                return new SchemaChangeResponseImpl()
                                    .setSchemaId(client.system_update_column_family(def.getThriftColumnFamilyDefinition()));
                            }
                        }, RunOnce.get());
    }
    
    @Override
    public OperationResult<SchemaChangeResult> updateColumnFamily(final Properties props) throws ConnectionException {
        if (props.containsKey("keyspace") && props.get("keyspace").equals(getKeyspaceName())) { 
            throw new RuntimeException(
                    String.format("'keyspace' attribute must match keyspace name. Expected '%s' but got '%s'", 
                                  getKeyspaceName(), props.get("name")));
        }
        
        return connectionPool
                .executeWithFailover(
                        new AbstractKeyspaceOperationImpl<SchemaChangeResult>(
                                tracerFactory.newTracer(CassandraOperationType.ADD_COLUMN_FAMILY), (String)props.getProperty("name")) {
                            @Override
                            public SchemaChangeResult internalExecute(Client client, ConnectionContext context) throws Exception {
                                CfDef def = ThriftUtils.getThriftObjectFromProperties(CfDef.class, props);
                                def.setKeyspace(getKeyspaceName());
                                return new SchemaChangeResponseImpl().setSchemaId(client.system_update_column_family(def));
                            }
                        }, RunOnce.get());
    }

    @Override
    public Properties getKeyspaceProperties() throws ConnectionException {
        KeyspaceDefinition ksDef = this.describeKeyspace();
        if (ksDef == null)
            throw new NotFoundException(String.format("Keyspace '%s' not found", getKeyspaceName()));
        
        Properties props = new Properties();
        ThriftKeyspaceDefinitionImpl thriftKsDef = (ThriftKeyspaceDefinitionImpl)ksDef;
        try {
            for (Entry<Object, Object> prop : thriftKsDef.getProperties().entrySet()) {
                props.setProperty((String)prop.getKey(), (String) prop.getValue());
            }
        } catch (Exception e) {
            LOG.error(String.format("Error fetching properties for keyspace '%s'", getKeyspaceName()));
        }
        return props;
    }

    @Override
    public Properties getColumnFamilyProperties(String columnFamily) throws ConnectionException {
        KeyspaceDefinition ksDef = this.describeKeyspace();
        ColumnFamilyDefinition cfDef = ksDef.getColumnFamily(columnFamily);
        if (cfDef == null)
            throw new NotFoundException(String.format("Column family '%s' in keyspace '%s' not found", columnFamily, getKeyspaceName()));
        
        Properties props = new Properties();
        ThriftColumnFamilyDefinitionImpl thriftCfDef = (ThriftColumnFamilyDefinitionImpl)cfDef;
        try {
            for (Entry<Object, Object> prop : thriftCfDef.getProperties().entrySet()) {
                props.setProperty((String)prop.getKey(), (String) prop.getValue());
            }
        } catch (Exception e) {
            LOG.error("Error processing column family properties");
        }
        return props;    
    }
}
