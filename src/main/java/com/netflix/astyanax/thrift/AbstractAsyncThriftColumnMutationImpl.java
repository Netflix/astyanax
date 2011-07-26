package com.netflix.astyanax.thrift;

import com.netflix.astyanax.Clock;
import com.netflix.astyanax.Execution;
import com.netflix.astyanax.KeyspaceTracers;
import com.netflix.astyanax.connectionpool.ConnectionPool;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ExecutionHelper;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

class AbstractAsyncThriftColumnMutationImpl<K, C> extends AbstractThriftColumnMutationImpl {
    private final ThriftAsyncKeyspaceImpl thriftAsyncKeyspace;
    private final ConnectionPool<Cassandra.AsyncClient> connectionPool;
    private final ColumnFamily<K, C> columnFamily;
    private final String keyspaceName;
    private final ExecutorService executorService;
    private final KeyspaceTracers tracers;

    public AbstractAsyncThriftColumnMutationImpl(ThriftAsyncKeyspaceImpl thriftAsyncKeyspace, ConnectionPool<Cassandra.AsyncClient> connectionPool, ColumnFamily<K, C> columnFamily, String keyspaceName,
                                                 ExecutorService executorService, KeyspaceTracers tracers, K rowKey, C column, Clock clock,
                                                 ConsistencyLevel readConsistencyLevel, ConsistencyLevel writeConsistencyLevel) {
        super(columnFamily.getKeySerializer().toByteBuffer(rowKey), columnFamily.getColumnSerializer().toByteBuffer(column), clock, readConsistencyLevel, writeConsistencyLevel);
        this.thriftAsyncKeyspace = thriftAsyncKeyspace;
        this.connectionPool = connectionPool;
        this.columnFamily = columnFamily;
        this.keyspaceName = keyspaceName;
        this.executorService = executorService;
        this.tracers = tracers;
    }

    @Override
    public Execution<Void> incrementCounterColumn(final long amount) {
        return new Execution<Void>() {
            @Override
            public OperationResult<Void> execute() throws ConnectionException {
                return ExecutionHelper.blockingExecute(this);
            }

            @Override
            public Future<OperationResult<Void>> executeAsync() throws ConnectionException {
                AbstractAsyncOperationImpl<Void, Cassandra.AsyncClient.add_call> operation = new AbstractAsyncOperationImpl<Void, Cassandra.AsyncClient.add_call>(
                    connectionPool,
                        keyspaceName
                ) {
                    @Override
                    public void startOperation(Cassandra.AsyncClient client) throws ConnectionException {
                        CounterColumn c = new CounterColumn();
                        c.setValue(amount);
                        c.setName(column);

                        try {
                            client.add(key,
                               ThriftConverter.getColumnParent(columnFamily, null),
                               c,
                               ThriftConverter.ToThriftConsistencyLevel(writeConsistencyLevel),
                               this);
                        }
                        catch (TException e) {
                            throw ThriftConverter.ToConnectionPoolException(e);
                        }
                    }

                    @Override
                    public Void finishOperation(Cassandra.AsyncClient.add_call response) throws ConnectionException {
                        return null;
                    }

                    @Override
                    public void trace(OperationResult<Void> result) {
                        tracers.incMutation(thriftAsyncKeyspace, result.getHost(), result.getLatency());
                    }
                };
                return AsyncFuture.make(executorService, operation);
            }
        };
    }

    @Override
    public Execution<Void> deleteColumn() {
        return new Execution<Void>() {
            @Override
            public OperationResult<Void> execute() throws ConnectionException {
                return ExecutionHelper.blockingExecute(this);
            }

            @Override
            public Future<OperationResult<Void>> executeAsync() throws ConnectionException {
                AbstractAsyncOperationImpl<Void, Cassandra.AsyncClient.remove_call> operation = new AbstractAsyncOperationImpl<Void, Cassandra.AsyncClient.remove_call>(
                    connectionPool,
                        keyspaceName
                ) {
                    @Override
                    public void startOperation(Cassandra.AsyncClient client) throws ConnectionException {
                        try {
                            client.remove(key,
                                    new org.apache.cassandra.thrift.ColumnPath()
                                        .setColumn_family(columnFamily.getName())
                                        .setColumn(column),
                                    clock.getCurrentTime(),
                                    ThriftConverter.ToThriftConsistencyLevel(writeConsistencyLevel),
                                    this);
                        }
                        catch (TException e) {
                            throw ThriftConverter.ToConnectionPoolException(e);
                        }
                    }

                    @Override
                    public Void finishOperation(Cassandra.AsyncClient.remove_call response) throws ConnectionException {
                        return null;
                    }

                    @Override
                    public void trace(OperationResult<Void> result) {
                        tracers.incMutation(thriftAsyncKeyspace, result.getHost(), result.getLatency());
                    }
                };
                return AsyncFuture.make(executorService, operation);
            }
        };
    }

    @Override
    public Execution<Void> insertValue(final ByteBuffer value,
            final Integer ttl) {
        return new Execution<Void>() {
            @Override
            public OperationResult<Void> execute() throws ConnectionException {
                return ExecutionHelper.blockingExecute(this);
            }

            @Override
            public Future<OperationResult<Void>> executeAsync() throws ConnectionException {
                AbstractAsyncOperationImpl<Void, Cassandra.AsyncClient.insert_call> operation = new AbstractAsyncOperationImpl<Void, Cassandra.AsyncClient.insert_call>(
                    connectionPool,
                        keyspaceName
                ) {
                    @Override
                    public void startOperation(Cassandra.AsyncClient client) throws ConnectionException {
                        try {
                            org.apache.cassandra.thrift.Column c = new org.apache.cassandra.thrift.Column();
                            c.setName(column);
                            c.setValue(value);
                            c.setTimestamp(clock.getCurrentTime());
                            if (ttl != null) {
                                c.setTtl(ttl);
                            }

                            client.insert(key,
                                   ThriftConverter.getColumnParent(columnFamily, null),
                                    c,
                                    ThriftConverter.ToThriftConsistencyLevel(writeConsistencyLevel),
                                    this);
                        }
                        catch (TException e) {
                            throw ThriftConverter.ToConnectionPoolException(e);
                        }
                    }

                    @Override
                    public Void finishOperation(Cassandra.AsyncClient.insert_call response) throws ConnectionException {
                        return null;
                    }

                    @Override
                    public void trace(OperationResult<Void> result) {
                        tracers.incMutation(thriftAsyncKeyspace, result.getHost(), result.getLatency());
                    }
                };
                return AsyncFuture.make(executorService, operation);
            }
        };
    }
}
