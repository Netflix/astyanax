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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.CounterColumn;

import com.google.common.base.Function;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.netflix.astyanax.AstyanaxConfiguration;
import com.netflix.astyanax.CassandraOperationType;
import com.netflix.astyanax.ColumnMutation;
import com.netflix.astyanax.Execution;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.KeyspaceTracerFactory;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.SerializerPackage;
import com.netflix.astyanax.WriteAheadEntry;
import com.netflix.astyanax.WriteAheadLog;
import com.netflix.astyanax.connectionpool.ConnectionPool;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.TokenRange;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.connectionpool.impl.TokenRangeImpl;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.retry.RetryPolicy;
import com.netflix.astyanax.serializers.SerializerPackageImpl;
import com.netflix.astyanax.serializers.UnknownComparatorException;
import com.netflix.astyanax.thrift.ddl.ThriftKeyspaceDefinitionImpl;

public final class ThriftKeyspaceImpl implements Keyspace {

    private final ConnectionPool<Cassandra.Client> connectionPool;
    private final AstyanaxConfiguration config;
    private final String ksName;
    private final IPartitioner partitioner;
    private final ExecutorService executor;
    private final KeyspaceTracerFactory tracerFactory;
    private final Cache<String, Object> cache;

    public ThriftKeyspaceImpl(String ksName, ConnectionPool<Cassandra.Client> pool, AstyanaxConfiguration config,
            final KeyspaceTracerFactory tracerFactory) {
        this.connectionPool = pool;
        this.config = config;
        this.ksName = ksName;
        this.partitioner = config.getPartitioner();
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
        return new AbstractThriftMutationBatchImpl(config.getClock()) {
            private ConsistencyLevel consistencyLevel = config.getDefaultWriteConsistencyLevel();
            private RetryPolicy retry = config.getRetryPolicy().duplicate();
            private Host pinnedHost;
            private WriteAheadLog wal;

            @Override
            public MutationBatch pinToHost(Host host) {
                this.pinnedHost = host;
                return this;
            }

            @Override
            public MutationBatch setConsistencyLevel(ConsistencyLevel consistencyLevel) {
                this.consistencyLevel = consistencyLevel;
                return this;
            }

            @Override
            public OperationResult<Void> execute() throws ConnectionException {
                WriteAheadEntry walEntry = null;
                if (wal != null) {
                    walEntry = wal.createEntry();
                    walEntry.writeMutation(this);
                }
                try {
                    OperationResult<Void> result = executeOperation(
                            new AbstractKeyspaceOperationImpl<Void>(
                                    tracerFactory.newTracer(CassandraOperationType.BATCH_MUTATE), pinnedHost,
                                    getKeyspaceName()) {
                                @Override
                                public Void internalExecute(Client client) throws Exception {
                                    client.batch_mutate(getMutationMap(),
                                            ThriftConverter.ToThriftConsistencyLevel(consistencyLevel));
                                    discardMutations();
                                    return null;
                                }

                                @Override
                                public Token getToken() {
                                    if (getMutationMap().size() == 1) {
                                        return toToken(getMutationMap().keySet().iterator().next());
                                    }
                                    else
                                        return null;
                                }
                            }, retry);

                    if (walEntry != null) {
                        wal.removeEntry(walEntry);
                    }
                    return result;
                }
                catch (ConnectionException exception) {
                    throw exception;
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

            @Override
            public MutationBatch withRetryPolicy(RetryPolicy retry) {
                this.retry = retry;
                return this;
            }

            @Override
            public MutationBatch usingWriteAheadLog(WriteAheadLog manager) {
                this.wal = manager;
                return this;
            }
        };
    }

    @Override
    public List<TokenRange> describeRing() throws ConnectionException {
        return executeOperation(
                new AbstractKeyspaceOperationImpl<List<TokenRange>>(tracerFactory
                        .newTracer(CassandraOperationType.DESCRIBE_RING), getKeyspaceName()) {
                    @Override
                    public List<TokenRange> internalExecute(Cassandra.Client client)
                            throws Exception {
                        return Lists.transform(client.describe_ring(getKeyspaceName()),
                                new Function<org.apache.cassandra.thrift.TokenRange, TokenRange>() {
                                    @Override
                                    public TokenRange apply(
                                            org.apache.cassandra.thrift.TokenRange tr) {
                                        return new TokenRangeImpl(tr.getStart_token(), tr.getEnd_token(), tr.getEndpoints());
                                    }
                                });
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
        return new ThriftColumnFamilyQueryImpl<K, C>(executor, tracerFactory, this, connectionPool, partitioner, cf,
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
                                    public Token getToken() {
                                        return toToken(columnFamily.getKeySerializer().toByteBuffer(rowKey));
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
                                    public Token getToken() {
                                        return toToken(columnFamily.getKeySerializer().toByteBuffer(rowKey));
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
                                    public Token getToken() {
                                        return toToken(columnFamily.getKeySerializer().toByteBuffer(rowKey));
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
                                    public Token getToken() {
                                        return toToken(columnFamily.getKeySerializer().toByteBuffer(rowKey));
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
        return executeOperation(
                new AbstractKeyspaceOperationImpl<Void>(tracerFactory.newTracer(CassandraOperationType.TRUNCATE),
                        getKeyspaceName()) {
                    @Override
                    public Void internalExecute(Cassandra.Client client) throws Exception {
                        client.truncate(columnFamily.getName());
                        return null;
                    }
                }, config.getRetryPolicy().duplicate());
    }

    private Token toToken(ByteBuffer key) {
        return (Token) partitioner.getToken(key).token;
    }

    private <R> OperationResult<R> executeOperation(Operation<Cassandra.Client, R> operation, RetryPolicy retry)
            throws OperationException, ConnectionException {
        return connectionPool.executeWithFailover(operation, retry);
    }

}
