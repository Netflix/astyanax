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

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.joda.time.DateTime;

import com.google.common.base.Supplier;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.astyanax.connectionpool.ConnectionPool;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.NodeDiscovery;

/**
 * Re-discover the ring on a fixed interval to identify new nodes or changes to
 * the ring tokens. <br/> <br/>
 * 
 * The class is started by the AstyanaxContext and uses a thread within an executor to repeatedly update the list of hosts required by a {@link ConnectionPool} using 
 * {@link ConnectionPool#setHosts(java.util.Collection)}. Note that the host source / supplier is passed in as an argument. 
 * @author elandau
 * 
 * @param <CL> 
 */
public class NodeDiscoveryImpl implements NodeDiscovery {
    private final ConnectionPool<?> connectionPool;
    private final ScheduledExecutorService executor;
    private boolean bOwnedExecutor = false;
    private final int interval;
    private final String name;
    private final Supplier<List<Host>> hostSupplier;
    private final AtomicReference<DateTime> lastUpdateTime = new AtomicReference<DateTime>();
    private final AtomicReference<Exception> lastException = new AtomicReference<Exception>();
    private final AtomicLong refreshCounter = new AtomicLong();
    private final AtomicLong errorCounter = new AtomicLong();

    public NodeDiscoveryImpl(
            String name, 
            int interval, 
            Supplier<List<Host>> hostSupplier,
            ConnectionPool<?> connectionPool) {
        this(name, interval, hostSupplier, connectionPool, 
              Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true).build()));
        
        bOwnedExecutor = true;
    }
    
    public NodeDiscoveryImpl(
            String name, 
            int interval, 
            Supplier<List<Host>> hostSupplier,
            ConnectionPool<?> connectionPool,
            ScheduledExecutorService executor) {
        this.connectionPool = connectionPool;
        this.interval       = interval;
        this.hostSupplier   = hostSupplier;
        this.name           = name;
        this.executor       = executor;
    }

    /**
	 * 
	 */
    @Override
    public void start() {
        update();

        executor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                Thread.currentThread().setName("RingDescribeAutoDiscovery");
                update();
            }
        }, interval, interval, TimeUnit.MILLISECONDS);

        NodeDiscoveryMonitorManager.getInstance().registerMonitor(name, this);
    }

    @Override
    public void shutdown() {
        if (bOwnedExecutor)
            executor.shutdown();
        NodeDiscoveryMonitorManager.getInstance().unregisterMonitor(name, this);
    }

    private void update() {
        try {
            connectionPool.setHosts(hostSupplier.get());
            refreshCounter.incrementAndGet();
            lastUpdateTime.set(new DateTime());
        }
        catch (Exception e) {
            errorCounter.incrementAndGet();
            lastException.set(e);
        }
    }

    @Override
    public DateTime getLastRefreshTime() {
        return lastUpdateTime.get();
    }

    @Override
    public long getRefreshCount() {
        return refreshCounter.get();
    }

    @Override
    public Exception getLastException() {
        return lastException.get();
    }

    @Override
    public long getErrorCount() {
        return errorCounter.get();
    }

    @Override
    public String getRawHostList() {
        try {
            return hostSupplier.get().toString();
        }
        catch (Exception e) {
            return e.getMessage();
        }
    }
}
