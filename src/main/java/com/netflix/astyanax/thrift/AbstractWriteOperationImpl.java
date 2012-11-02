package com.netflix.astyanax.thrift;

import com.netflix.astyanax.CassandraOperationTracer;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.OperationPreprocessor;
import com.netflix.astyanax.consistency.WriteConsistencyLevelProvider;
import com.netflix.astyanax.model.ConsistencyLevel;

/**
 * @author Max Morozov
 */
public abstract class AbstractWriteOperationImpl<R> extends AbstractKeyspaceOperationImpl<R>  implements WriteConsistencyLevelProvider {
    private ConsistencyLevel consistencyLevel;

    public AbstractWriteOperationImpl(CassandraOperationTracer tracer, Host pinnedHost, String keyspaceName, ConsistencyLevel consistencyLevel) {
        super(tracer, pinnedHost, keyspaceName);
        this.consistencyLevel = consistencyLevel;
    }

    public AbstractWriteOperationImpl(CassandraOperationTracer tracer, String keyspaceName, ConsistencyLevel consistencyLevel) {
        super(tracer, keyspaceName);
        this.consistencyLevel = consistencyLevel;
    }

    @Override
    public ConsistencyLevel getWriteConsistencyLevel() {
        return consistencyLevel;
    }

    @Override
    public void setWriteConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
    }

    @Override
    public void preprocess(OperationPreprocessor preprocessor) {
        preprocessor.handleWriteConsistency(this);
    }

}
