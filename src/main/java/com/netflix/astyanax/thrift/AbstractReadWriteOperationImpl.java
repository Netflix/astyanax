package com.netflix.astyanax.thrift;

import com.netflix.astyanax.CassandraOperationTracer;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.OperationPreprocessor;
import com.netflix.astyanax.consistency.ReadConsistencyLevelProvider;
import com.netflix.astyanax.consistency.WriteConsistencyLevelProvider;
import com.netflix.astyanax.model.ConsistencyLevel;

/**
 * @author Max Morozov
 */
public abstract class AbstractReadWriteOperationImpl<R> extends AbstractKeyspaceOperationImpl<R> 
        implements ReadConsistencyLevelProvider, WriteConsistencyLevelProvider {
    private ConsistencyLevel readConsistencyLevel;
    private ConsistencyLevel writeConsistencyLevel;

    public AbstractReadWriteOperationImpl(CassandraOperationTracer tracer, Host pinnedHost, String keyspaceName, 
                                          ConsistencyLevel readConsistencyLevel, ConsistencyLevel writeConsistencyLevel) {
        super(tracer, pinnedHost, keyspaceName);
        this.readConsistencyLevel = readConsistencyLevel;
        this.writeConsistencyLevel = writeConsistencyLevel;
    }

    public AbstractReadWriteOperationImpl(CassandraOperationTracer tracer, String keyspaceName, 
                                          ConsistencyLevel readConsistencyLevel, ConsistencyLevel writeConsistencyLevel) {
        super(tracer, keyspaceName);
        this.readConsistencyLevel = readConsistencyLevel;
        this.writeConsistencyLevel = writeConsistencyLevel;
    }

    @Override
    public ConsistencyLevel getReadConsistencyLevel() {
        return readConsistencyLevel;
    }

    @Override
    public ConsistencyLevel getWriteConsistencyLevel() {
        return writeConsistencyLevel;
    }

    @Override
    public void setReadConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.readConsistencyLevel = consistencyLevel;
    }

    @Override
    public void setWriteConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.writeConsistencyLevel = consistencyLevel;
    }

    @Override
    public void preprocess(OperationPreprocessor preprocessor) {
        preprocessor.handleReadConsistency(this);
        preprocessor.handleWriteConsistency(this);
    }

}
