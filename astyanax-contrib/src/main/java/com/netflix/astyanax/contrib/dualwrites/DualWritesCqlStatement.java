package com.netflix.astyanax.contrib.dualwrites;

import java.util.Collections;

import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.cql.CqlPreparedStatement;
import com.netflix.astyanax.cql.CqlStatement;
import com.netflix.astyanax.cql.CqlStatementResult;
import com.netflix.astyanax.model.ConsistencyLevel;

public class DualWritesCqlStatement implements CqlStatement {

    private final CqlStatement primary;
    private final CqlStatement secondary;
    private final DualWritesStrategy execStrategy;
    private final DualKeyspaceMetadata ksMd;

    public DualWritesCqlStatement(CqlStatement primaryCql, CqlStatement secondarycql, DualWritesStrategy strategy, DualKeyspaceMetadata keyspaceMd) {
        primary = primaryCql;
        secondary = secondarycql;
        execStrategy = strategy;
        ksMd = keyspaceMd;
    }

    @Override
    public CqlStatement withConsistencyLevel(ConsistencyLevel cl) {
        primary.withConsistencyLevel(cl);
        secondary.withConsistencyLevel(cl);
        return this;
    }

    @Override
    public CqlStatement withCql(String cql) {
        primary.withCql(cql);
        secondary.withCql(cql);
        return this;
    }

    @Override
    public CqlPreparedStatement asPreparedStatement() {
        CqlPreparedStatement pstmtPrimary = primary.asPreparedStatement();
        CqlPreparedStatement pstmtSecondary = primary.asPreparedStatement();
        return new DualWritesCqlPreparedStatement(pstmtPrimary, pstmtSecondary, execStrategy, ksMd);
    }

    @Override
    public OperationResult<CqlStatementResult> execute() throws ConnectionException {
        
        WriteMetadata writeMd = new WriteMetadata(ksMd, null, null);
        return execStrategy.wrapExecutions(primary, secondary, Collections.singletonList(writeMd)).execute();
    }

    @Override
    public ListenableFuture<OperationResult<CqlStatementResult>> executeAsync() throws ConnectionException {
        WriteMetadata writeMd = new WriteMetadata(ksMd, null, null);
        return execStrategy.wrapExecutions(primary, secondary, Collections.singletonList(writeMd)).executeAsync();
    }
}
