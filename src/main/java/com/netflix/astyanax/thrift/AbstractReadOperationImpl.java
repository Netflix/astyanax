package com.netflix.astyanax.thrift;

import com.netflix.astyanax.CassandraOperationTracer;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.OperationPreprocessor;
import com.netflix.astyanax.consistency.ReadConsistencyLevelProvider;
import com.netflix.astyanax.model.ConsistencyLevel;

/**
 * @author Max Morozov
 */
public abstract class AbstractReadOperationImpl<R> extends AbstractKeyspaceOperationImpl<R> implements ReadConsistencyLevelProvider {
    private ConsistencyLevel consistencyLevel;
    
    public AbstractReadOperationImpl(CassandraOperationTracer tracer, Host pinnedHost, String keyspaceName, ConsistencyLevel consistencyLevel) {
        super(tracer, pinnedHost, keyspaceName);
        this.consistencyLevel = consistencyLevel;
    }

    public AbstractReadOperationImpl(CassandraOperationTracer tracer, String keyspaceName, ConsistencyLevel consistencyLevel) {
        super(tracer, keyspaceName);
        this.consistencyLevel = consistencyLevel;
    }

    @Override
    public ConsistencyLevel getReadConsistencyLevel() {
        return consistencyLevel;
    }

    @Override
    public void setReadConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
    }

    @Override
    public void preprocess(OperationPreprocessor preprocessor) {
        preprocessor.handleReadConsistency(this);
    }

}