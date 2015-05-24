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
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import com.netflix.astyanax.connectionpool.ConnectionPool;
import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.LatencyScoreStrategy.Instance;
import com.netflix.astyanax.connectionpool.LatencyScoreStrategy.Listener;
import com.netflix.astyanax.connectionpool.exceptions.BadRequestException;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionAbortedException;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.HostDownException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.connectionpool.exceptions.OperationTimeoutException;
import com.netflix.astyanax.connectionpool.exceptions.PoolTimeoutException;
import com.netflix.astyanax.connectionpool.exceptions.TimeoutException;
import com.netflix.astyanax.connectionpool.exceptions.TokenRangeOfflineException;
import com.netflix.astyanax.connectionpool.exceptions.TransportException;
import com.netflix.astyanax.connectionpool.exceptions.UnknownException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.RoundRobinConnectionPoolImpl;
import com.netflix.astyanax.retry.RunOnce;
import com.netflix.astyanax.test.ProbabalisticFunction;
import com.netflix.astyanax.test.TestClient;
import com.netflix.astyanax.test.TestConnectionFactory;
import com.netflix.astyanax.test.TestConstants;
import com.netflix.astyanax.test.TestDriver;
import com.netflix.astyanax.test.TestHostType;
import com.netflix.astyanax.test.TestOperation;

public class Stress {
    private static Logger LOG = LoggerFactory.getLogger(Stress.class);

    /**
     * @param args
     */
    public static void main(String[] args) {
        ConnectionPoolConfigurationImpl config;
        config = new ConnectionPoolConfigurationImpl(TestConstants.CLUSTER_NAME
                + "_" + TestConstants.KEYSPACE_NAME);
//        config.setMaxConns(100);
        config.setMaxFailoverCount(-1);
        config.setMaxTimeoutWhenExhausted(1000);
        config.setMaxConnsPerHost(25);
        config.setInitConnsPerHost(0);
        config.setTimeoutWindow(5000);
        config.setMaxTimeoutCount(10);
        config.setRetrySuspendWindow(5000);
        config.setLatencyScoreStrategy(new EmaLatencyScoreStrategyImpl(1000, 0, 20));
        // config.setRetryBackoffStrategy(new
        // ExponentialRetryBackoffStrategy(20, 1000, 2000));

        final ConnectionPoolMonitor monitor   = new CountingConnectionPoolMonitor();
        TestConnectionFactory factory         = new TestConnectionFactory(config, monitor);
        final ConnectionPool<TestClient> pool = new RoundRobinConnectionPoolImpl<TestClient>(config, factory, monitor);
        pool.start();

        final List<Host> hosts = Lists.newArrayList(
                new Host("127.0.0.1",  TestHostType.GOOD_FAST.ordinal()),
                new Host("127.0.0.2",  TestHostType.GOOD_FAST.ordinal()),
                new Host("127.0.0.3",  TestHostType.GOOD_FAST.ordinal()),
                new Host("127.0.0.4",  TestHostType.GOOD_FAST.ordinal()),
                new Host("127.0.0.5",  TestHostType.GOOD_FAST.ordinal()),
                new Host("127.0.0.6",  TestHostType.GOOD_FAST.ordinal()),
                new Host("127.0.0.7",  TestHostType.GOOD_FAST.ordinal()),
                new Host("127.0.0.8",  TestHostType.GOOD_FAST.ordinal()),
//                new Host("127.0.0.9",  TestHostType.GOOD_SLOW.ordinal()),
                new Host("127.0.0.10",  TestHostType.SWAP_EVERY_200.ordinal()),
                new Host("127.0.0.11", TestHostType.ALTERNATING_SOCKET_TIMEOUT_200.ordinal())
//                new Host("127.0.0.12", TestHostType.ALTERNATING_SOCKET_TIMEOUT_200.ordinal()),
//                new Host("127.0.0.13",  TestHostType.CONNECT_FAIL_FIRST_TWO.ordinal())
                );
        
        for (Host host : hosts) {
            pool.addHost(host, true);
        }
                
        final Map<Host, AtomicLong> counts = new TreeMap<Host, AtomicLong>();
        for (HostConnectionPool<TestClient> p : pool.getActivePools()) {
            counts.put(p.getHost(), new AtomicLong());
        }
        System.out.println(monitor.toString());
        
        final AtomicBoolean timeoutsEnabled = new AtomicBoolean(false);
        final AtomicLong lastOperationCount = new AtomicLong();
        
        EmaLatencyScoreStrategyImpl latency = new EmaLatencyScoreStrategyImpl(1000, 0, 10);
        final Instance sampler = latency.createInstance();
        latency.start(new Listener() {
            @Override
            public void onUpdate() {
            }
            @Override
            public void onReset() {
            }
        });
        final Function<TestDriver, Void> function = new ProbabalisticFunction.Builder<TestDriver, Void>()
            .withDefault(new Function<TestDriver, Void>() {
                public Void apply(TestDriver arg0) {
                    return null;
                }
            })
            .withAlways(new Runnable() {
                public void run() {
                    think(10, 30); 
                }
            })
//            .withProbability(0.0001, new Function<TestDriver, Void>() {
//                public Void apply(@Nullable TestDriver arg0) {
//                    if (timeoutsEnabled.get()) {
//                        think(1100, 0);
//                        throw new RuntimeException(new TimeoutException("TimeoutException"));
//                    }
//                    return null;
//                }
//            })
//            .withProbability(0.0001, new Function<TestDriver, Void>() {
//                public Void apply(@Nullable TestDriver arg0) {
//                    throw new RuntimeException(new UnknownException(new Exception("UnknownExceptionDescription")));
//                }
//            })
//            .withProbability(0.0001, new Function<TestDriver, Void>() {
//                public Void apply(@Nullable TestDriver arg0) {
//                    think(1000, 0);
//                    throw new RuntimeException(new OperationTimeoutException("OperationTimeoutException"));
//                }
//            })
//            .withProbability(0.0001, new Function<TestDriver, Void>() {
//                public Void apply(@Nullable TestDriver arg0) {
//                    throw new RuntimeException(new HostDownException("HostDownException"));
//                }
//            })
//            .withProbability(0.01, new Function<TestDriver, Void>() {
//                public Void apply(@Nullable TestDriver arg0) {
//                    throw new RuntimeException(new ConnectionAbortedException("ConnectionAbortedException"));
//                }
//            })
//            .withProbability(0.0001, new Function<TestDriver, Void>() {
//                public Void apply(@Nullable TestDriver arg0) {
//                    throw new RuntimeException(new BadRequestException("BadRequestException"));
//                }
//            })
//            .withProbability(0.0001, new Function<TestDriver, Void>() {
//                public Void apply(@Nullable TestDriver arg0) {
//                    throw new RuntimeException(new TokenRangeOfflineException("TokenRangeOfflineException"));
//                }
//            })
//            .withProbability(0.0001, new Function<TestDriver, Void>() {
//                public Void apply(@Nullable TestDriver arg0) {
//                    throw new RuntimeException(new TransportException("TransportException"));
//                }
//            })
        .build();
        
        final List<HostConnectionPool<TestClient>> hostPools = Lists.newArrayList(pool.getActivePools());
        
        final TestDriver driver = new TestDriver.Builder()
            .withIterationCount(0)
            .withThreadCount(200)
//            .withFutures(100,  TimeUnit.MILLISECONDS)
            .withCallsPerSecondSupplier(Suppliers.ofInstance(200))
//            .withFutures(100, TimeUnit.MILLISECONDS)
            .withCallback(new Function<TestDriver, Void>() {
                public Void apply(final TestDriver driver) {
                    long startTime = System.nanoTime();
                    try {
                        pool.executeWithFailover(new TestOperation() {
                            public String execute(TestClient client) throws ConnectionException, OperationException {
                                try {
                                    function.apply(driver);
                                    return null;
                                }
                                catch (RuntimeException e) {
                                    if (e.getCause() instanceof ConnectionException)
                                        throw (ConnectionException)e.getCause();
                                    throw e;
                                }
                            }
                        }, new RunOnce());
                    } catch (PoolTimeoutException e) {
                        LOG.info(e.getMessage());
                    } catch (ConnectionException e) {
                    } finally {
                        sampler.addSample((System.nanoTime() - startTime)/1000000);
                    }
                    
                    return null;
                }
            })
            
            // 
            //  Event to turn timeouts on/off
            //
            .withRecurringEvent(10,  TimeUnit.SECONDS,  new Function<TestDriver, Void>() {
                @Override
                public Void apply(TestDriver driver) {
                    timeoutsEnabled.getAndSet(!timeoutsEnabled.get());
//                    LOG.info("Toggle timeouts " + timeoutsEnabled.get());
                    return null;
                }
            })
            
            //
            //  Print status information
            //
            .withRecurringEvent(1,  TimeUnit.SECONDS,  new Function<TestDriver, Void>() {
                @Override
                public Void apply(TestDriver driver) {
                    long opCount = lastOperationCount.get();
                    lastOperationCount.set(driver.getOperationCount());
                    
                    System.out.println("" + driver.getRuntime() + "," + sampler.getScore() + "," + (lastOperationCount.get() - opCount));
                    System.out.println(monitor.toString());
                    System.out.println(monitor.toString());
                    for (HostConnectionPool<TestClient> host : pool.getPools()) {
                        System.out.println("   " + host.toString());
                    }
                    return null;
                }
            })
            
            //
            //  Remove a random host
            //
            .withRecurringEvent(10,  TimeUnit.SECONDS,  new Function<TestDriver, Void>() {
                @Override
                public Void apply(TestDriver driver) {
//                    System.out.println("Latency: " + sampler.getScore());
//                    
//                    List<Host> newHosts = Lists.newArrayList(hosts);
//                    newHosts.remove(new Random().nextInt(hosts.size()));
//                    pool.setHosts(newHosts);
//                    
//                    System.out.println(monitor.toString());
//                    for (HostConnectionPool<TestClient> host : pool.getPools()) {
//                        System.out.println("   " + host.toString());
//                    }
                    return null;
                }
            })
            .build();
        
        driver.start();
        try {
            driver.await();
        } catch (InterruptedException e) {
        }
    }

    private static void think(int min, int max) {
        try {
            if (max > min) {
                Thread.sleep(min + new Random().nextInt(max - min));
            } else {
                Thread.sleep(min);
            }
        } catch (InterruptedException e) {
        }
    }
}
