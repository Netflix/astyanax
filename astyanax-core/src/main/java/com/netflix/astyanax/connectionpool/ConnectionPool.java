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
package com.netflix.astyanax.connectionpool;

import java.util.List;
import java.util.Collection;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.connectionpool.impl.Topology;
import com.netflix.astyanax.partitioner.Partitioner;
import com.netflix.astyanax.retry.RetryPolicy;

/**
 * Base interface for a pool of connections. A concrete connection pool will
 * track hosts in a cluster.
 * 
 * @author elandau
 * @param <CL>
 */
public interface ConnectionPool<CL> {
    /**
     * Add a host to the connection pool.
     * 
     * @param host
     * @returns True if host was added or false if host already exists
     * @throws ConnectionException
     */
    boolean addHost(Host host, boolean refresh);

    /**
     * Remove a host from the connection pool. Any pending connections will be
     * allowed to complete
     * 
     * @returns True if host was removed or false if host does not exist
     * @param host
     */
    boolean removeHost(Host host, boolean refresh);

    /**
     * @return Return true if the host is up
     * @param host
     */
    boolean isHostUp(Host host);

    /**
     * @return Return true if host is contained within the connection pool
     * @param host
     */
    boolean hasHost(Host host);

    /**
     * @return Return list of active hosts on which connections can be created
     */
    List<HostConnectionPool<CL>> getActivePools();

    /**
     * @return Get all pools
     */
    List<HostConnectionPool<CL>> getPools();
    
    /**
     * Set the complete set of hosts in the ring
     * @param hosts
     */
    void setHosts(Collection<Host> hosts);

    /**
     * @return Return an immutable connection pool for this host
     * @param host
     */
    HostConnectionPool<CL> getHostPool(Host host);

    /**
     * Execute an operation with failover within the context of the connection
     * pool. The operation will only fail over for connection pool errors and
     * not application errors.
     * 
     * @param <R>
     * @param op
     * @param token
     * @throws ConnectionException
     * @throws OperationException
     */
    <R> OperationResult<R> executeWithFailover(Operation<CL, R> op, RetryPolicy retry) throws ConnectionException,
            OperationException;

    /**
     * Shut down the connection pool and terminate all existing connections
     */
    void shutdown();

    /**
     * Setup the connection pool and start any maintenance threads
     */
    void start();
    
    /**
     * @return Return the internal topology which represents the partitioning of data across hosts in the pool
     */
    Topology<CL> getTopology();
    
    /**
     * @return Return the partitioner used by this pool.  
     */
    Partitioner getPartitioner();
}
