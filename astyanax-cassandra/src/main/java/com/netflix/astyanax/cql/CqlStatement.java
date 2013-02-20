package com.netflix.astyanax.cql;

import com.netflix.astyanax.Execution;
import com.netflix.astyanax.model.ConsistencyLevel;

public interface CqlStatement extends Execution<CqlStatementResult> {
    public CqlStatement withConsistencyLevel(ConsistencyLevel cl);
    public CqlStatement withCql(String cql);
    public CqlPreparedStatement asPreparedStatement();
}
