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

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import com.netflix.astyanax.connectionpool.ConnectionFactory;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.ExecuteWithFailover;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NoAvailableHostsException;

/**
 * Connection pool that partitions connections by the hosts which own the token
 * being operated on. When a token is not available or an operation is known to
 * span multiple tokens (such as a batch mutate or an index query) host pools
 * are picked using round robin.
 * 
 * This implementation takes an optimistic approach which is optimized for a
 * well functioning ring with all nodes up and keeps downed hosts in the
 * internal data structures.
 * 
 * @author elandau
 * 
 * @param <CL>
 */
public class TokenAwareConnectionPoolImpl<CL> extends
        AbstractHostPartitionConnectionPool<CL> {

    private AtomicInteger roundRobinCounter = new AtomicInteger(
            new Random().nextInt(997));

    public TokenAwareConnectionPoolImpl(
            ConnectionPoolConfiguration configuration,
            ConnectionFactory<CL> factory, ConnectionPoolMonitor monitor) {
        super(configuration, factory, monitor);
    }

    @SuppressWarnings("unchecked")
    public <R> ExecuteWithFailover<CL, R> newExecuteWithFailover(
            Operation<CL, R> op) throws ConnectionException {
        List<HostConnectionPool<CL>> pools;
        boolean isSorted = false;

        if (op.getPinnedHost() != null) {
            HostConnectionPool<CL> pool = hosts.get(op.getPinnedHost());
            if (pool == null) {
                throw new NoAvailableHostsException("Host "
                        + op.getPinnedHost() + " not active");
            }
            pools = Arrays.<HostConnectionPool<CL>> asList(pool);
        } else {
            HostConnectionPoolPartition<CL> partition = topology
                    .getPartition(op.getToken());
            pools = partition.getPools();
            isSorted = partition.isSorted();
        }

        return new RoundRobinExecuteWithFailover<CL, R>(config, monitor, pools,
                isSorted ? 0 : roundRobinCounter.incrementAndGet());
    }
}
