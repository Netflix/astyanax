package com.netflix.astyanax;

import com.netflix.astyanax.model.ColumnFamily;

/**
 * TODO: Rename to AstyanaxTracerFactory
 * 
 * @author elandau
 * 
 */
public interface KeyspaceTracerFactory {
    /**
     * Create a tracer for cluster level operations
     * 
     * @param type
     * @return
     */
    CassandraOperationTracer newTracer(CassandraOperationType type);

    /**
     * Create a tracer for a column family operation
     * 
     * @param type
     * @param columnFamily
     * @return
     */
    CassandraOperationTracer newTracer(CassandraOperationType type, ColumnFamily<?, ?> columnFamily);
}
