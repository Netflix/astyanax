/*******************************************************************************
 * Copyright 2011 Netflix
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.netflix.astyanax.test;

import java.util.List;

import com.netflix.astyanax.AstyanaxConfiguration;
import com.netflix.astyanax.ColumnMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.SerializerPackage;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.TokenRange;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.retry.RetryPolicy;
import com.netflix.astyanax.serializers.SerializerPackageImpl;

public class TestKeyspace implements Keyspace {
    private String keyspaceName;
    private List<TokenRange> tokenRange;

    public TestKeyspace(String name) {
        this.keyspaceName = name;
    }

    public void setTokenRange(List<TokenRange> tokens) {
        this.tokenRange = tokens;
    }

    @Override
    public String getKeyspaceName() {
        return this.keyspaceName;
    }

    @Override
    public List<TokenRange> describeRing() throws ConnectionException {
        return this.tokenRange;
    }

    @Override
    public MutationBatch prepareMutationBatch() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <K, C> ColumnFamilyQuery<K, C> prepareQuery(ColumnFamily<K, C> cf) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <K, C> ColumnMutation prepareColumnMutation(
            ColumnFamily<K, C> columnFamily, K rowKey, C column) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AstyanaxConfiguration getConfig() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public KeyspaceDefinition describeKeyspace() throws ConnectionException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SerializerPackage getSerializerPackage(String columnFamily,
            boolean ignoreErrors) {
        return SerializerPackageImpl.DEFAULT_SERIALIZER_PACKAGE;
    }

    @Override
    public OperationResult<Void> testOperation(Operation<?, ?> operation)
            throws ConnectionException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <K, C> OperationResult<Void> truncateColumnFamily(
            ColumnFamily<K, C> columnFamily) throws OperationException,
            ConnectionException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public OperationResult<Void> testOperation(Operation<?, ?> operation,
            RetryPolicy retry) throws ConnectionException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<TokenRange> describeRing(boolean cached) throws ConnectionException {
        // TODO Auto-generated method stub
        return null;
    }
}
