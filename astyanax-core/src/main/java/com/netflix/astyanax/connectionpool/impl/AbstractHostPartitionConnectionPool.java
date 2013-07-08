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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;

import org.cliffc.high_scale_lib.NonBlockingHashMap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.astyanax.connectionpool.ConnectionFactory;
import com.netflix.astyanax.connectionpool.ConnectionPool;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.ExecuteWithFailover;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.LatencyScoreStrategy.Listener;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.TokenRange;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.partitioner.Partitioner;
import com.netflix.astyanax.retry.RetryPolicy;

/**
 * Base for all connection pools that keep a separate pool of connections for
 * each host. <br/> <br/>
 * 
 * <p>
 * <b> Set of host connection pools </b>  <br/>
 * The class maintains the set of all {@link HostConnectionPool}s for all hosts in the token ring using a non blocking hashmap and a {@link Topology} 
 * The hashmap tracks the basic set of hosts and their corresponding connection pools. The topology is used to track the internal state of the token ring
 * for the cassandra cluster. <br/>
 * The class uses these 2 structures to determine whether there has been a change to the system when a host joins or leaves the ring, 
 * or even if an existing host just receives an update for the token ring partition that it owns. Hence the class can actively rebuild all the partitions
 * associated with each as and when there is a change.  <br/>
 * 
 * See {@link #addHost(Host, boolean)} {@link #removeHost(Host, boolean)} {@link #setHosts(Collection)} for host changes and the corresponding trigger to {@link #rebuildPartitions()}
 * when this happens.  <br/>
 * 
 * Note that when the connection pool is started it fetches the list of seed hosts from config (if any) and then inits it's data structures using these seed hosts. <br/>
 * It also employs a listener to the latency score updates so  that it can rebuild partitions as and when it receives updates 
 * </p>
 * 
 * <p>
 * <b> Execute with failover </b> <br/>
 * 
 * The class also provides a basic implementation of {@link #executeWithFailover(Operation, RetryPolicy)} by repeatedly consulting the {@link RetryPolicy}. Note that extending classes 
 * must provide impl for the actual execute with failover. e.g  the {@link RoundRobinConnectionPoolImpl} fails over to the next {@link HostConnectionPool} in the list. 
 * </p>
 * 
 * @author elandau
 * 
 * @param <CL>  
 * 
 * @see {@link RoundRobinConnectionPoolImpl}  {@link BagOfConnectionsConnectionPoolImpl} {@link TokenAwareConnectionPoolImpl} for details on impls that extend this class
 * @see {@link TokenPartitionedTopology} for how the internal topology structure is maintained across the set of hosts
 */
public abstract class AbstractHostPartitionConnectionPool<CL> implements ConnectionPool<CL>,
        SimpleHostConnectionPool.Listener<CL> {
    protected final NonBlockingHashMap<Host, HostConnectionPool<CL>> hosts;
    protected final ConnectionPoolConfiguration                      config;
    protected final ConnectionFactory<CL>                            factory;
    protected final ConnectionPoolMonitor                            monitor;
    protected final Topology<CL>                                     topology;
    protected final Partitioner                                      partitioner;

    /**
     * @param config
     * @param factory
     * @param monitor
     */
    public AbstractHostPartitionConnectionPool(ConnectionPoolConfiguration config, ConnectionFactory<CL> factory,
            ConnectionPoolMonitor monitor) {
        this.config     = config;
        this.factory    = factory;
        this.monitor    = monitor;
        this.hosts      = new NonBlockingHashMap<Host, HostConnectionPool<CL>>();
        this.topology   = new TokenPartitionedTopology<CL>(config.getPartitioner(), config.getLatencyScoreStrategy());
        this.partitioner = config.getPartitioner();
    }

    /**
     * Starts the conn pool and resources associated with it
     */
    @Override
    public void start() {
        ConnectionPoolMBeanManager.getInstance().registerMonitor(config.getName(), this);

        String seeds = config.getSeeds();
        if (seeds != null && !seeds.isEmpty()) {
            setHosts(config.getSeedHosts());
        }

        config.getLatencyScoreStrategy().start(new Listener() {
            @Override
            public void onUpdate() {
                rebuildPartitions();
            }

            @Override
            public void onReset() {
                rebuildPartitions();
            }
        });
    }

    /**
     * Clean up resources associated with the conn pool
     */
    @Override
    public void shutdown() {
        ConnectionPoolMBeanManager.getInstance().unregisterMonitor(config.getName(), this);

        for (Entry<Host, HostConnectionPool<CL>> pool : hosts.entrySet()) {
            pool.getValue().shutdown();
        }

        config.getLatencyScoreStrategy().shutdown();
        config.shutdown();
    }

    protected HostConnectionPool<CL> newHostConnectionPool(Host host, ConnectionFactory<CL> factory,
            ConnectionPoolConfiguration config) {
        return new SimpleHostConnectionPool<CL>(host, factory, monitor, config, this);
    }

    /**
     * Host is marked as down
     * @param pool
     */
    @Override
    public void onHostDown(HostConnectionPool<CL> pool) {
        topology.suspendPool(pool);
    }

    /**
     * Host is marked as up
     * @param pool
     */
    @Override
    public void onHostUp(HostConnectionPool<CL> pool) {
        topology.resumePool(pool);
    }

    private static Comparator<TokenRange> compareByStartToken = new Comparator<TokenRange>() {
        @Override
        public int compare(TokenRange p1, TokenRange p2) {
            return p1.getStartToken().compareTo(p2.getStartToken());
        }
    };

    /**
     * Add host to the system. May need to rebuild the partition map of the system
     * @param host
     * @param refresh
     */
    @Override
    public final synchronized boolean addHost(Host host, boolean refresh) {
        // Already exists
        if (hosts.containsKey(host)) {
            // Check to see if we are adding token ranges or if the token ranges changed
            // which will force a rebuild of the token topology
            Host existingHost = hosts.get(host).getHost();
            if (existingHost.getTokenRanges().size() != host.getTokenRanges().size()) {
                existingHost.setTokenRanges(host.getTokenRanges());
                return true;
            }
            
            ArrayList<TokenRange> currentTokens = Lists.newArrayList(existingHost.getTokenRanges());
            ArrayList<TokenRange> newTokens     = Lists.newArrayList(host.getTokenRanges());
            Collections.sort(currentTokens, compareByStartToken);
            Collections.sort(newTokens,     compareByStartToken);
            for (int i = 0; i < currentTokens.size(); i++) {
                if (!currentTokens.get(i).getStartToken().equals(newTokens.get(i).getStartToken()) ||
                    !currentTokens.get(i).getEndToken().equals(newTokens.get(i).getEndToken())) {
                    return false;
                }
            }
            
            existingHost.setTokenRanges(host.getTokenRanges());
            return true;
        }
        else {
            HostConnectionPool<CL> pool = newHostConnectionPool(host, factory, config);
            if (null == hosts.putIfAbsent(host, pool)) {
                try {
                    monitor.onHostAdded(host, pool);
                    if (refresh) {
                        topology.addPool(pool);
                        rebuildPartitions();
                    }
                    pool.primeConnections(config.getInitConnsPerHost());
                }
                catch (Exception e) {
                    // Ignore, pool will have been marked down internally
                }
                return true;
            }
            else {
                return false;
            }
        }
    }

    
    /**
     * @return boolean
     */
    @Override
    public boolean isHostUp(Host host) {
        HostConnectionPool<CL> pool = hosts.get(host);
        if (pool != null) {
            return !pool.isReconnecting();
        }
        return false;
    }

    /**
     * @return boolean
     */
    @Override
    public boolean hasHost(Host host) {
        return hosts.containsKey(host);
    }

    /**
     * list of all active pools
     * @return {@link List<HostConnectionPool>}
     */
    @Override
    public List<HostConnectionPool<CL>> getActivePools() {
        return ImmutableList.copyOf(topology.getAllPools().getPools());
    }

    /**
     * @return {@link List<HostConnectionPool>}
     */
    @Override
    public List<HostConnectionPool<CL>> getPools() {
        return ImmutableList.copyOf(hosts.values());
    }

    /**
     * Remove host from the system. Shuts down pool associated with the host and rebuilds partition map
     * @param host
     * @param refresh
     */
    @Override
    public synchronized boolean removeHost(Host host, boolean refresh) {
        HostConnectionPool<CL> pool = hosts.remove(host);
        if (pool != null) {
            topology.removePool(pool);
            rebuildPartitions();
            monitor.onHostRemoved(host);
            pool.shutdown();
            return true;
        }
        else {
            return false;
        }
    }

    /**
     * @param host
     * @return {@link HostConnectionPool}
     */
    @Override
    public HostConnectionPool<CL> getHostPool(Host host) {
        return hosts.get(host);
    }

    /**
     * @param ring
     */
    @Override
    public synchronized void setHosts(Collection<Host> ring) {
        
        // Temporary list of hosts to remove. Any host not in the new ring will
        // be removed
        Set<Host> hostsToRemove = Sets.newHashSet(hosts.keySet());

        // Add new hosts.
        boolean changed = false;
        for (Host host : ring) {
            if (addHost(host, false))
                changed = true;
            hostsToRemove.remove(host);
        }

        // Remove any hosts that are no longer in the ring
        for (Host host : hostsToRemove) {
            removeHost(host, false);
            changed = true;
        }

        if (changed) {
            topology.setPools(hosts.values());
            rebuildPartitions();
        }
    }
    
    /**
     * Executes the operation using failover and retry strategy
     * @param op
     * @param retry
     * @return {@link OperationResult}
     */
    @Override
    public <R> OperationResult<R> executeWithFailover(Operation<CL, R> op, RetryPolicy retry)
            throws ConnectionException {
        retry.begin();
        ConnectionException lastException = null;
        do {
            try {
                OperationResult<R> result = newExecuteWithFailover(op).tryOperation(op);
                retry.success();
                return result;
            }
            catch (OperationException e) {
                retry.failure(e);
                throw e;
            }
            catch (ConnectionException e) {
                lastException = e;
            }
        } while (retry.allowRetry());
        retry.failure(lastException);
        throw lastException;
    }

    /**
     * Return a new failover context. The context captures the connection pool
     * state and implements the necessary failover logic.
     * 
     * @param <R>
     * @return
     * @throws ConnectionException
     */
    protected abstract <R> ExecuteWithFailover<CL, R> newExecuteWithFailover(Operation<CL, R> op)
            throws ConnectionException;

    /**
     * Called every time a host is added, removed or is marked as down
     */
    protected void rebuildPartitions() {
        topology.refresh();
    }
    
    /**
     * @return {@link Topology}
     */
    public Topology<CL> getTopology() {
        return topology;
    }
    
    /**
     * @return {@link Partitioner}
     */
    public Partitioner getPartitioner() {
        return this.partitioner;
    }
}
