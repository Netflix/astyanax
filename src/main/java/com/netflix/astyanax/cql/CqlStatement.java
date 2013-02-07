package com.netflix.astyanax.cql;

import com.netflix.astyanax.Execution;

public interface CqlStatement extends Execution<CqlStatementResult> {
    public CqlStatement withCql(String cql);
}
