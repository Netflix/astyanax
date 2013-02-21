package com.netflix.astyanax.thrift;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;

import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.CassandraOperationType;
import com.netflix.astyanax.connectionpool.ConnectionContext;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.query.AbstractPreparedCqlQuery;
import com.netflix.astyanax.query.CqlQuery;
import com.netflix.astyanax.query.PreparedCqlQuery;
import com.netflix.astyanax.thrift.model.ThriftCqlResultImpl;
import com.netflix.astyanax.thrift.model.ThriftCqlRowsImpl;

public abstract class AbstractThriftCqlQuery<K,C> implements CqlQuery<K,C> {
    boolean useCompression = false;
    ThriftColumnFamilyQueryImpl<K,C> cfQuery;
    String cql;
    ConsistencyLevel cl = ConsistencyLevel.CL_ONE;
    
    AbstractThriftCqlQuery(ThriftColumnFamilyQueryImpl<K,C> cfQuery, String cql) {
        this.cfQuery = cfQuery;
        this.cql = cql;
    }
    
    @Override
    public OperationResult<CqlResult<K, C>> execute() throws ConnectionException {
        return cfQuery.keyspace.connectionPool.executeWithFailover(
                new AbstractKeyspaceOperationImpl<CqlResult<K, C>>(cfQuery.keyspace.tracerFactory.newTracer(
                        CassandraOperationType.CQL, cfQuery.columnFamily), cfQuery.pinnedHost, cfQuery.keyspace.getKeyspaceName()) {
                    @Override
                    public CqlResult<K, C> internalExecute(Client client, ConnectionContext context) throws Exception {
                        org.apache.cassandra.thrift.CqlResult res = execute_cql_query(client);
                        switch (res.getType()) {
                        case ROWS:
                            return new ThriftCqlResultImpl<K, C>(new ThriftCqlRowsImpl<K, C>(res.getRows(),
                                    cfQuery.columnFamily.getKeySerializer(), cfQuery.columnFamily.getColumnSerializer()));
                        case INT:
                            return new ThriftCqlResultImpl<K, C>(res.getNum());
                        default:
                            return null;
                        }
                    }
                }, cfQuery.retry);
    }

    @Override
    public ListenableFuture<OperationResult<CqlResult<K, C>>> executeAsync() throws ConnectionException {
        return cfQuery.keyspace.executor.submit(new Callable<OperationResult<CqlResult<K, C>>>() {
            @Override
            public OperationResult<CqlResult<K, C>> call() throws Exception {
                return execute();
            }
        });
    }

    @Override
    public CqlQuery<K, C> useCompression() {
        useCompression = true;
        return this;
    }

    @Override
    public PreparedCqlQuery<K, C> asPreparedStatement() {
        return new AbstractPreparedCqlQuery<K, C>() {
            @Override
            public OperationResult<CqlResult<K, C>> execute() throws ConnectionException {
                return cfQuery.keyspace.connectionPool.executeWithFailover(
                        new AbstractKeyspaceOperationImpl<CqlResult<K, C>>(cfQuery.keyspace.tracerFactory.newTracer(
                                CassandraOperationType.CQL, cfQuery.columnFamily), cfQuery.pinnedHost, cfQuery.keyspace.getKeyspaceName()) {
                            @Override
                            public CqlResult<K, C> internalExecute(Client client, ConnectionContext state) throws Exception {
                                Integer id = (Integer)state.getMetadata(cql);
                                if (id == null) {
                                    org.apache.cassandra.thrift.CqlPreparedResult res = prepare_cql_query(client);
                                    id = res.getItemId();
                                    state.setMetadata(cql, id);
                                }

                                org.apache.cassandra.thrift.CqlResult res = execute_prepared_cql_query(client, id, getValues());
                                switch (res.getType()) {
                                case ROWS:
                                    return new ThriftCqlResultImpl<K, C>(new ThriftCqlRowsImpl<K, C>(res.getRows(),
                                            cfQuery.columnFamily.getKeySerializer(), cfQuery.columnFamily.getColumnSerializer()));
                                case INT:
                                    return new ThriftCqlResultImpl<K, C>(res.getNum());
                                    
                                default:
                                    return null;
                                }
                            }
                        }, cfQuery.retry);
            }

            @Override
            public ListenableFuture<OperationResult<CqlResult<K, C>>> executeAsync() throws ConnectionException {
                return cfQuery.executor.submit(new Callable<OperationResult<CqlResult<K, C>>>() {
                    @Override
                    public OperationResult<CqlResult<K, C>> call() throws Exception {
                        return execute();
                    }
                });
            }
        };
    }
    
    public CqlQuery<K, C> withConsistencyLevel(ConsistencyLevel cl) {
        this.cl = cl;
        return this;
    }
    
    protected abstract org.apache.cassandra.thrift.CqlPreparedResult prepare_cql_query(Client client) 
            throws InvalidRequestException, TException;
    
    protected abstract org.apache.cassandra.thrift.CqlResult execute_prepared_cql_query(Client client, int id, List<ByteBuffer> values) 
            throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException;
    
    protected abstract org.apache.cassandra.thrift.CqlResult execute_cql_query(Client client) 
            throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException;

}
