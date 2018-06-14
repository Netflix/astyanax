/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
