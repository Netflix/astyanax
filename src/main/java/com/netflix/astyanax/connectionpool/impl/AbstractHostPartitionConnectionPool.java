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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.cliffc.high_scale_lib.NonBlockingHashMap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.astyanax.connectionpool.ConnectionFactory;
import com.netflix.astyanax.connectionpool.ConnectionPool;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.ExecuteWithFailover;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.LatencyScoreStrategy;
import com.netflix.astyanax.connectionpool.LatencyScoreStrategy.Listener;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.retry.RetryPolicy;

/**
 * Base for all connection pools that keep a separate pool of connections for
 * each host.
 * 
 * @author elandau
 * 
 * @param <CL>
 */
public abstract class AbstractHostPartitionConnectionPool<CL> implements ConnectionPool<CL>,
        SimpleHostConnectionPool.Listener<CL> {
    protected final NonBlockingHashMap<Host, HostConnectionPool<CL>> hosts;
    protected final ConnectionPoolConfiguration config;
    protected final ConnectionFactory<CL> factory;
    protected final ConnectionPoolMonitor monitor;
    private final LatencyScoreStrategy latencyScoreStrategy;
    protected final Topology<CL> topology;

    public AbstractHostPartitionConnectionPool(ConnectionPoolConfiguration config, ConnectionFactory<CL> factory,
            ConnectionPoolMonitor monitor) {
        this.config = config;
        this.factory = factory;
        this.hosts = new NonBlockingHashMap<Host, HostConnectionPool<CL>>();
        this.monitor = monitor;
        this.latencyScoreStrategy = config.getLatencyScoreStrategy();
        this.topology = new TokenPartitionedTopology<CL>(this.latencyScoreStrategy);
    }

    @Override
    public void start() {
        ConnectionPoolMBeanManager.getInstance().registerMonitor(config.getName(), this);

        String seeds = config.getSeeds();
        if (seeds != null && !seeds.isEmpty()) {
            Map<BigInteger, List<Host>> ring = Maps.newHashMap();
            ring.put(new BigInteger("0"), config.getSeedHosts());
            setHosts(ring);
        }

        latencyScoreStrategy.start(new Listener() {
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

    @Override
    public void shutdown() {
        ConnectionPoolMBeanManager.getInstance().unregisterMonitor(config.getName(), this);

        for (Entry<Host, HostConnectionPool<CL>> pool : hosts.entrySet()) {
            pool.getValue().shutdown();
        }

        latencyScoreStrategy.shutdown();
    }

    protected HostConnectionPool<CL> newHostConnectionPool(Host host, ConnectionFactory<CL> factory,
            ConnectionPoolConfiguration config) {
        return new SimpleHostConnectionPool<CL>(host, factory, monitor, config, this);
    }

    @Override
    public void onHostDown(HostConnectionPool<CL> pool) {
        topology.suspendPool(pool);
    }

    @Override
    public void onHostUp(HostConnectionPool<CL> pool) {
        topology.resumePool(pool);
    }

    @Override
    public final boolean addHost(Host host, boolean refresh) {
        // Already exists
        if (hosts.get(host) != null) {
            return false;
        }

        HostConnectionPool<CL> pool = newHostConnectionPool(host, factory, config);
        if (null == hosts.putIfAbsent(host, pool)) {
            try {
                monitor.onHostAdded(host, pool);
                if (refresh) {
                    topology.addPool(pool);
                    rebuildPartitions();
                }
                pool.growConnections(config.getInitConnsPerHost());
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

    @Override
    public boolean isHostUp(Host host) {
        HostConnectionPool<CL> pool = hosts.get(host);
        if (pool != null) {
            return !pool.isShutdown();
        }
        return false;
    }

    @Override
    public boolean hasHost(Host host) {
        return hosts.containsKey(host);
    }

    @Override
    public List<HostConnectionPool<CL>> getActivePools() {
        return ImmutableList.copyOf(topology.getAllPools().getPools());
    }

    @Override
    public boolean removeHost(Host host, boolean refresh) {
        HostConnectionPool<CL> pool = hosts.remove(host);
        if (pool != null) {
            monitor.onHostRemoved(host);
            pool.shutdown();
            if (refresh) {
                topology.removePool(pool);
                rebuildPartitions();
            }
            return true;
        }
        else {
            return false;
        }
    }

    @Override
    public HostConnectionPool<CL> getHostPool(Host host) {
        return hosts.get(host);
    }

    @Override
    public synchronized void setHosts(Map<BigInteger, List<Host>> ring) {
        // Temporary list of hosts to remove. Any host not in the new ring will
        // be removed
        Set<Host> hostsToRemove = Sets.newHashSet(hosts.keySet());

        // Add new hosts.
        for (Map.Entry<BigInteger, List<Host>> entry : ring.entrySet()) {
            for (Host host : entry.getValue()) {
                addHost(host, false);
                hostsToRemove.remove(host);
            }
        }

        // Remove any hosts that are no longer in the ring
        for (Host host : hostsToRemove) {
            removeHost(host, false);
        }

        // Recreate the toplogy
        Map<BigInteger, Collection<HostConnectionPool<CL>>> tokens = Maps.newHashMap();
        for (Map.Entry<BigInteger, List<Host>> entry : ring.entrySet()) {
            Set<HostConnectionPool<CL>> pools = Sets.newHashSet();
            for (Host host : entry.getValue()) {
                pools.add(getHostPool(host));
            }
            tokens.put(entry.getKey(), pools);
        }

        topology.setPools(tokens);
    }

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
}
