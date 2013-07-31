package com.netflix.astyanax;

public enum CassandraOperationType {
    ATOMIC_BATCH_MUTATE (CassandraOperationCategory.WRITE), 
    BATCH_MUTATE        (CassandraOperationCategory.WRITE), 
    GET_ROW             (CassandraOperationCategory.READ), 
    GET_ROWS_RANGE      (CassandraOperationCategory.READ), 
    GET_ROWS_SLICE      (CassandraOperationCategory.READ), 
    GET_ROWS_BY_INDEX   (CassandraOperationCategory.READ), 
    GET_COLUMN          (CassandraOperationCategory.READ), 
    CQL                 (CassandraOperationCategory.CQL), 
    DESCRIBE_RING       (CassandraOperationCategory.OTHER), 
    COUNTER_MUTATE      (CassandraOperationCategory.WRITE), 
    COLUMN_MUTATE       (CassandraOperationCategory.WRITE), 
    COLUMN_DELETE       (CassandraOperationCategory.WRITE), 
    COLUMN_INSERT       (CassandraOperationCategory.WRITE), 
    GET_COLUMN_COUNT    (CassandraOperationCategory.READ), 
    COPY_TO             (CassandraOperationCategory.WRITE), 
    DESCRIBE_KEYSPACE   (CassandraOperationCategory.OTHER), 
    TRUNCATE            (CassandraOperationCategory.OTHER), 
    DESCRIBE_CLUSTER    (CassandraOperationCategory.OTHER), 
    DESCRIBE_VERSION    (CassandraOperationCategory.OTHER), 
    DESCRIBE_SNITCH     (CassandraOperationCategory.OTHER), 
    DESCRIBE_PARTITIONER(CassandraOperationCategory.OTHER), 
    DESCRIBE_SCHEMA_VERSION(CassandraOperationCategory.OTHER), 
    GET_VERSION         (CassandraOperationCategory.OTHER), 
    DROP_COLUMN_FAMILY  (CassandraOperationCategory.OTHER), 
    DESCRIBE_KEYSPACES  (CassandraOperationCategory.OTHER),
    DROP_KEYSPACE       (CassandraOperationCategory.OTHER),
    ADD_COLUMN_FAMILY   (CassandraOperationCategory.OTHER), 
    UPDATE_COLUMN_FAMILY(CassandraOperationCategory.OTHER), 
    ADD_KEYSPACE        (CassandraOperationCategory.OTHER), 
    UPDATE_KEYSPACE     (CassandraOperationCategory.OTHER), 
    SET_KEYSPACE        (CassandraOperationCategory.OTHER), 
    TEST                (CassandraOperationCategory.OTHER),
    
    ;
    CassandraOperationCategory category;
    
    CassandraOperationType(CassandraOperationCategory category) {
        this.category = category;
    }
    
    public CassandraOperationCategory getCategory() {
        return this.category;
    }
}
