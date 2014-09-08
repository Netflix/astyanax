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

import java.util.Collection;
import java.util.List;

import com.netflix.astyanax.connectionpool.ConnectionPool;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.connectionpool.impl.Topology;
import com.netflix.astyanax.partitioner.Partitioner;
import com.netflix.astyanax.retry.RetryPolicy;

public class TestConnectionPool implements ConnectionPool<TestClient> {

    Collection<Host> ring;

    public Collection<Host> getHosts() {
        return this.ring;
    }

    @Override
    public boolean addHost(Host host, boolean refresh) {
        return true;
    }

    @Override
    public boolean removeHost(Host host, boolean refresh) {
        return true;
    }

    @Override
    public void setHosts(Collection<Host> ring) {
        this.ring = ring;
    }

    @Override
    public <R> OperationResult<R> executeWithFailover(
            Operation<TestClient, R> op, RetryPolicy retry)
            throws ConnectionException, OperationException {
        return null;
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void start() {
    }

    @Override
    public boolean isHostUp(Host host) {
        return false;
    }

    @Override
    public boolean hasHost(Host host) {
        return false;
    }

    @Override
    public HostConnectionPool<TestClient> getHostPool(Host host) {
        return null;
    }

    @Override
    public List<HostConnectionPool<TestClient>> getActivePools() {
        return null;
    }

    @Override
    public List<HostConnectionPool<TestClient>> getPools() {
        return null;
    }

    @Override
    public Topology<TestClient> getTopology() {
        return null;
    }

    @Override
    public Partitioner getPartitioner() {
        // TODO Auto-generated method stub
        return null;
    }

}
