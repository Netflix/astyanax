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

import org.apache.cassandra.thrift.Cassandra;

import com.netflix.astyanax.CassandraOperationTracer;
import com.netflix.astyanax.connectionpool.ConnectionContext;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

public abstract class AbstractOperationImpl<R> implements Operation<Cassandra.Client, R> {
    private final CassandraOperationTracer tracer;
    private Host pinnedHost;

    public AbstractOperationImpl(CassandraOperationTracer tracer, Host host) {
        this.tracer = tracer;
        this.pinnedHost = host;
    }

    public AbstractOperationImpl(CassandraOperationTracer tracer) {
        this.tracer = tracer;
        this.pinnedHost = null;
    }

    public void setPinnedHost(Host host) {
        this.pinnedHost = host;
    }
    
    @Override
    public ByteBuffer getRowKey() {
        return null;
    }

    @Override
    public String getKeyspace() {
        return null;
    }

    @Override
    public R execute(Cassandra.Client client, ConnectionContext state) throws ConnectionException {
        try {
            tracer.start();
            R result = internalExecute(client, state);
            tracer.success();
            return result;
        }
        catch (Exception e) {
            ConnectionException ce = ThriftConverter.ToConnectionPoolException(e);
            tracer.failure(ce);
            throw ce;
        }
    }

    @Override
    public Host getPinnedHost() {
        return pinnedHost;
    }

    protected abstract R internalExecute(Cassandra.Client client, ConnectionContext state) throws Exception;
}
