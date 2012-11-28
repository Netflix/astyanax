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
     * Return true if the host is up
     * 
     * @param host
     * @return
     */
    boolean isHostUp(Host host);

    /**
     * Return true if host is contained within the connection pool
     * 
     * @param host
     * @return
     */
    boolean hasHost(Host host);

    /**
     * Return list of active hosts on which connections can be created
     * 
     * @return
     */
    List<HostConnectionPool<CL>> getActivePools();

    /**
     * Get all pools
     * @return
     */
    List<HostConnectionPool<CL>> getPools();
    
    /**
     * Set the complete set of hosts in the ring
     * @param hosts
     */
    void setHosts(Collection<Host> hosts);

    /**
     * Return an immutable connection pool for this host
     * 
     * @param host
     * @return
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
     * @return
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
     * Return the internal topology which represents the partitioning of data across hosts in the pool
     * @return
     */
    Topology<CL> getTopology();
}
