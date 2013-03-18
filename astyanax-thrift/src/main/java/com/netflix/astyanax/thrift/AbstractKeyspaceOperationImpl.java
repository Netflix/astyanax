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
package com.netflix.astyanax.thrift;

import java.nio.ByteBuffer;

import com.netflix.astyanax.CassandraOperationTracer;
import com.netflix.astyanax.connectionpool.Host;

public abstract class AbstractKeyspaceOperationImpl<R> extends AbstractOperationImpl<R> {
    private String keyspaceName;

    public AbstractKeyspaceOperationImpl(CassandraOperationTracer tracer, Host pinnedHost, String keyspaceName) {
        super(tracer, pinnedHost);
        this.keyspaceName = keyspaceName;
    }

    public AbstractKeyspaceOperationImpl(CassandraOperationTracer tracer, String keyspaceName) {
        super(tracer);
        this.keyspaceName = keyspaceName;
    }

    @Override
    public String getKeyspace() {
        return this.keyspaceName;
    }
    
    @Override
    public ByteBuffer getRowKey() {
        return null;
    }

}
