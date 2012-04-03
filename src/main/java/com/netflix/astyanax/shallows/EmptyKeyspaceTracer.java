package com.netflix.astyanax.shallows;

import com.netflix.astyanax.CassandraOperationTracer;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

public class EmptyKeyspaceTracer implements CassandraOperationTracer {

    private static EmptyKeyspaceTracer instance = new EmptyKeyspaceTracer();

    public static EmptyKeyspaceTracer getInstance() {
        return instance;
    }

    private EmptyKeyspaceTracer() {

    }

    @Override
    public CassandraOperationTracer start() {
        return this;
    }

    @Override
    public void success() {
    }

    @Override
    public void failure(ConnectionException e) {
    }

}
