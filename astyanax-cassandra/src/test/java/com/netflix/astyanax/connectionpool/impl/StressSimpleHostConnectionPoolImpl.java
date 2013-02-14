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

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.netflix.astyanax.connectionpool.Connection;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.test.TestClient;
import com.netflix.astyanax.test.TestConnectionFactory;
import com.netflix.astyanax.test.TestHostType;

public class StressSimpleHostConnectionPoolImpl {
    private static final Logger LOG = Logger.getLogger(Stress.class);

    public static class NoOpListener implements
            SimpleHostConnectionPool.Listener<TestClient> {
        @Override
        public void onHostDown(HostConnectionPool<TestClient> pool) {
        }

        @Override
        public void onHostUp(HostConnectionPool<TestClient> pool) {
        }

    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        ConnectionPoolConfigurationImpl config = new ConnectionPoolConfigurationImpl(
                "cluster_keyspace");
        config.setMaxConnsPerHost(3);

        CountingConnectionPoolMonitor monitor = new CountingConnectionPoolMonitor();

        Host host = new Host("127.0.0.1", TestHostType.GOOD_IMMEDIATE.ordinal());
        final SimpleHostConnectionPool<TestClient> pool = new SimpleHostConnectionPool<TestClient>(
                host, new TestConnectionFactory(null, monitor), monitor,
                config, new NoOpListener());

        int numThreads = 100;
        final int numOperations = 100;
        final int timeout = 2000;

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        for (int i = 0; i < numThreads; i++) {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < numOperations; i++) {
                        Connection<TestClient> conn = null;
                        try {
                            conn = pool.borrowConnection(timeout);
                            think(10, 10);
                        } catch (OperationException e) {
                            // e.printStackTrace();
                        } catch (ConnectionException e) {
                            // e.printStackTrace();
                        } finally {
                            conn.getHostConnectionPool().returnConnection(conn);
                        }
                    }
                }
            });
        }

        try {
            executor.shutdown();
            while (!executor.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
                LOG.info(pool.toString());
            }
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        LOG.info("**** FINISHED ****");
        LOG.info(pool.toString());
    }

    private static void think(int min, int max) {
        try {
            if (max > min) {
                Thread.sleep(min + new Random().nextInt(max - min));
            } else {
                Thread.sleep(min);
            }
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
