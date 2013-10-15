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
package com.netflix.astyanax.connectionpool.impl;

import com.netflix.astyanax.connectionpool.*;
import com.netflix.astyanax.connectionpool.exceptions.*;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Connection pool implementation using simple round robin. <br/> <br/>
 * It maintains a rotating index over a collection of {@link HostConnectionPool}(s) maintained using a {@link Topology} that reflects the given 
 * partitioned set of pools. Note that the impl uses the <b>pinned host</b> on the operation if it finds one. If there is none, then it uses 
 * all the host connection pools in the topology.
 * 
 * @see {@link RoundRobinExecuteWithFailover} for more details on how failover works with round robin connections.
 * @see {@link Topology} for details on where the collection of {@link HostConnectionPool}(s) are maintained. 
 * @see {@link AbstractHostPartitionConnectionPool} for the base impl of {@link ConnectionPool}
 * 
 * @author elandau
 * 
 * @param <CL>
 */
public class RoundRobinConnectionPoolImpl<CL> extends AbstractHostPartitionConnectionPool<CL> {

    private final AtomicInteger roundRobinCounter = new AtomicInteger(new Random().nextInt(997));
    private static final int MAX_RR_COUNTER = Integer.MAX_VALUE/2;

    public RoundRobinConnectionPoolImpl(ConnectionPoolConfiguration config, ConnectionFactory<CL> factory,
            ConnectionPoolMonitor monitor) {
        super(config, factory, monitor);
    }

    @SuppressWarnings("unchecked")
    public <R> ExecuteWithFailover<CL, R> newExecuteWithFailover(Operation<CL, R> operation) throws ConnectionException {
        try {
            if (operation.getPinnedHost() != null) {
                HostConnectionPool<CL> pool = hosts.get(operation.getPinnedHost());
                if (pool == null) {
                    throw new NoAvailableHostsException("Host " + operation.getPinnedHost() + " not active");
                }
                return new RoundRobinExecuteWithFailover<CL, R>(config, monitor,
                        Arrays.<HostConnectionPool<CL>> asList(pool), 0);
            }
            
            int index = roundRobinCounter.incrementAndGet();
            if (index > MAX_RR_COUNTER) {
                roundRobinCounter.set(0);
            }
            
            return new RoundRobinExecuteWithFailover<CL, R>(config, monitor, topology.getAllPools().getPools(), index);
        }
        catch (ConnectionException e) {
            monitor.incOperationFailure(e.getHost(), e);
            throw e;
        }
    }
}
