package com.netflix.astyanax.shallows;

import com.netflix.astyanax.CassandraOperationTracer;
import com.netflix.astyanax.CassandraOperationType;
import com.netflix.astyanax.KeyspaceTracerFactory;
import com.netflix.astyanax.model.ColumnFamily;

public class EmptyKeyspaceTracerFactory implements KeyspaceTracerFactory {
    public static EmptyKeyspaceTracerFactory instance = new EmptyKeyspaceTracerFactory();

    public static EmptyKeyspaceTracerFactory getInstance() {
        return instance;
    }

    private EmptyKeyspaceTracerFactory() {

    }

    @Override
    public CassandraOperationTracer newTracer(CassandraOperationType type) {
        return EmptyKeyspaceTracer.getInstance();
    }

    @Override
    public CassandraOperationTracer newTracer(CassandraOperationType type, ColumnFamily<?, ?> columnFamily) {
        return EmptyKeyspaceTracer.getInstance();
    }

    public String toString() {
        return "EmptyKeyspaceTracerFactory";
    }
}
