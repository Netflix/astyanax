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
 * Connection pool implementation using simple round robin.
 * 
 * @author elandau
 * 
 * @param <CL>
 */
public class RoundRobinConnectionPoolImpl<CL> extends
        AbstractHostPartitionConnectionPool<CL> {

    private final AtomicInteger roundRobinCounter = new AtomicInteger(
            new Random().nextInt(997));

    public RoundRobinConnectionPoolImpl(ConnectionPoolConfiguration config,
            ConnectionFactory<CL> factory, ConnectionPoolMonitor monitor) {
        super(config, factory, monitor);
    }

    @SuppressWarnings("unchecked")
    public <R> ExecuteWithFailover<CL, R> newExecuteWithFailover(
            Operation<CL, R> operation) throws ConnectionException {
        if (operation.getPinnedHost() != null) {
            HostConnectionPool<CL> pool = hosts.get(operation.getPinnedHost());
            if (pool == null) {
                this.monitor.incNoHosts();
                throw new NoAvailableHostsException("Host "
                        + operation.getPinnedHost() + " not active");
            }
            return new RoundRobinExecuteWithFailover<CL, R>(config, monitor,
                    Arrays.<HostConnectionPool<CL>> asList(pool), 0);
        }
        return new RoundRobinExecuteWithFailover<CL, R>(config, monitor,
                topology.getAllPools().getPools(),
                roundRobinCounter.incrementAndGet());
    }
}
