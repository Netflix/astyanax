package com.netflix.astyanax.cql;

import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Rows;

public interface CqlStatementResult {

    long asCount();

    <K, C> Rows<K, C> getRows(ColumnFamily<K, C> columnFamily);

    CqlSchema getSchema();

}
