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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
 * the ring tokens.
 * 
 * @author elandau
 * 
 * @param <CL>
 */
public class NodeDiscoveryImpl implements NodeDiscovery {
    private final ConnectionPool<?> connectionPool;
    private final ScheduledExecutorService executor = Executors
            .newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                    .setDaemon(true).build());
    private final int interval;
    private final String name;
    private final Supplier<Map<BigInteger, List<Host>>> tokenRangeSupplier;
    private final AtomicReference<DateTime> lastUpdateTime = new AtomicReference<DateTime>();
    private final AtomicReference<Exception> lastException = new AtomicReference<Exception>();
    private final AtomicLong refreshCounter = new AtomicLong();
    private final AtomicLong errorCounter = new AtomicLong();

    public NodeDiscoveryImpl(String name, int interval,
            Supplier<Map<BigInteger, List<Host>>> tokenRangeSupplier,
            ConnectionPool<?> connectionPool) {
        this.connectionPool = connectionPool;
        this.interval = interval;
        this.tokenRangeSupplier = tokenRangeSupplier;
        this.name = name;
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
        executor.shutdown();
        NodeDiscoveryMonitorManager.getInstance().unregisterMonitor(name, this);
    }

    private void update() {
        try {
            connectionPool.setHosts(tokenRangeSupplier.get());
            refreshCounter.incrementAndGet();
            lastUpdateTime.set(new DateTime());
        } catch (Exception e) {
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
        StringBuilder sb = new StringBuilder();
        Map<BigInteger, List<Host>> hosts;
        try {
            hosts = tokenRangeSupplier.get();
            boolean first = true;
            for (Entry<BigInteger, List<Host>> token : hosts.entrySet()) {
                if (!first)
                    sb.append(",");
                else
                    first = false;
                sb.append(token).append(":[");
                boolean firstHost = true;
                for (Host host : token.getValue()) {
                    if (!firstHost)
                        sb.append(",");
                    else
                        firstHost = false;
                    sb.append(host.getHostName());
                }
                sb.append("]");
            }
            return sb.toString();
        } catch (Exception e) {
            return e.getMessage();
        }
    }
}
