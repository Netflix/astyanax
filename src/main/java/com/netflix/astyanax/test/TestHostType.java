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
package com.netflix.astyanax.test;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.BadRequestException;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionAbortedException;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.HostDownException;
import com.netflix.astyanax.connectionpool.exceptions.OperationTimeoutException;
import com.netflix.astyanax.connectionpool.exceptions.TimeoutException;
import com.netflix.astyanax.connectionpool.exceptions.TransportException;
import com.netflix.astyanax.connectionpool.impl.OperationResultImpl;

public enum TestHostType {
    ALWAYS_DOWN {
        @Override
        public <R> OperationResult<R> execute(
                HostConnectionPool<TestClient> pool, Operation<TestClient, R> op)
                throws ConnectionException {
            throw new TransportException("TransportException");
        }

        @Override
        public void open(long timeout) throws ConnectionException {
            throw new TransportException("TransportException");
        }
    },

    CONNECT_WITH_UNCHECKED_EXCEPTION {
        @Override
        public <R> OperationResult<R> execute(
                HostConnectionPool<TestClient> pool, Operation<TestClient, R> op)
                throws ConnectionException {
            return null;
        }

        @Override
        public void open(long timeout) throws ConnectionException {
            throw new RuntimeException("UNCHECKED_OPEN_EXCEPTION");
        }
    },

    CONNECT_TIMEOUT {
        @Override
        public <R> OperationResult<R> execute(
                HostConnectionPool<TestClient> pool, Operation<TestClient, R> op)
                throws ConnectionException {
            throw new IllegalStateException(CONNECT_TIMEOUT.name());
        }

        @Override
        public void open(long timeout) throws ConnectionException {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
            }
            throw new TimeoutException("TimeoutException");
        }
    },

    CONNECT_TRANSPORT_ERROR {
        @Override
        public <R> OperationResult<R> execute(
                HostConnectionPool<TestClient> pool, Operation<TestClient, R> op)
                throws ConnectionException {
            throw new IllegalStateException(CONNECT_TRANSPORT_ERROR.name());
        }

        @Override
        public void open(long timeout) throws ConnectionException {
            throw new TransportException(CONNECT_TRANSPORT_ERROR.name());
        }
    },

    CONNECT_FAIL_FIRST {
        private AtomicInteger count = new AtomicInteger(0);

        @Override
        public <R> OperationResult<R> execute(
                HostConnectionPool<TestClient> pool, Operation<TestClient, R> op)
                throws ConnectionException {
            return new OperationResultImpl<R>(pool.getHost(), op.execute(null, null),
                    0);
        }

        @Override
        public void open(long timeout) throws ConnectionException {
            if (count.getAndIncrement() == 0) {
                throw new TransportException("connection refused");
            }
        }
    },

    CONNECT_FAIL_FIRST_TWO {
        private AtomicInteger count = new AtomicInteger(0);

        @Override
        public <R> OperationResult<R> execute(
                HostConnectionPool<TestClient> pool, Operation<TestClient, R> op)
                throws ConnectionException {
            return new OperationResultImpl<R>(pool.getHost(), op.execute(null, null),
                    0);
        }

        @Override
        public void open(long timeout) throws ConnectionException {
            if (count.incrementAndGet() <= 2) {
                throw new TransportException("connection refused");
            }
        }
    },

    LOST_CONNECTION {
        @Override
        public <R> OperationResult<R> execute(
                HostConnectionPool<TestClient> pool, Operation<TestClient, R> op)
                throws ConnectionException {
            throw new TransportException("TransportException");
        }

        @Override
        public void open(long timeout) throws ConnectionException {
        }
    },

    SOCKET_TIMEOUT_AFTER10 {
        private AtomicInteger count = new AtomicInteger(0);
        private int failAfter = 10;

        @Override
        public <R> OperationResult<R> execute(
                HostConnectionPool<TestClient> pool, Operation<TestClient, R> op)
                throws ConnectionException {
            if (count.incrementAndGet() >= failAfter) {
                throw new TimeoutException("TimeoutException");
            }
            return new OperationResultImpl<R>(pool.getHost(), op.execute(null, null),
                    think(5));
        }

        @Override
        public void open(long timeout) throws ConnectionException {
            if (count.get() >= failAfter) {
                think(1000);
                throw new TimeoutException("Timeout");
                // count.set(0);
            }
        }
    },

    OPERATION_TIMEOUT {
        @Override
        public <R> OperationResult<R> execute(
                HostConnectionPool<TestClient> pool, Operation<TestClient, R> op)
                throws ConnectionException {
            throw new OperationTimeoutException("TimedOutException");
        }

        @Override
        public void open(long timeout) throws ConnectionException {
        }
    },

    SOCKET_TIMEOUT {
        @Override
        public <R> OperationResult<R> execute(
                HostConnectionPool<TestClient> pool, Operation<TestClient, R> op)
                throws ConnectionException {
            think(2000);
            throw new TimeoutException("SocketTimeException");
        }

        @Override
        public void open(long timeout) throws ConnectionException {
        }
    },
    ALTERNATING_SOCKET_TIMEOUT_200 {
        private AtomicLong counter = new AtomicLong(0);
        @Override
        public <R> OperationResult<R> execute(
                HostConnectionPool<TestClient> pool, Operation<TestClient, R> op)
                throws ConnectionException {
            if (counter.incrementAndGet() / 200 % 2 == 1) {
                think(200);
                throw new TimeoutException("SocketTimeException");
            }
            else {
                return new OperationResultImpl<R>(pool.getHost(), op.execute(null, null), think(0));
            }
        }

        @Override
        public void open(long timeout) throws ConnectionException {
        }
    },
    CONNECT_BAD_REQUEST_EXCEPTION {
        @Override
        public <R> OperationResult<R> execute(
                HostConnectionPool<TestClient> pool, Operation<TestClient, R> op)
                throws ConnectionException {
            throw new TransportException("TransportException");
        }

        @Override
        public void open(long timeout) throws ConnectionException {
            throw new BadRequestException("BadRequestException");
        }
    },

    GOOD_SLOW {
        @Override
        public <R> OperationResult<R> execute(
                HostConnectionPool<TestClient> pool, Operation<TestClient, R> op)
                throws ConnectionException {
            return new OperationResultImpl<R>(pool.getHost(), op.execute(null, null),
                    think(500));
        }

        @Override
        public void open(long timeout) throws ConnectionException {
            think(500);
        }
    },

    GOOD_FAST {

        @Override
        public <R> OperationResult<R> execute(
                HostConnectionPool<TestClient> pool, Operation<TestClient, R> op)
                throws ConnectionException {
            return new OperationResultImpl<R>(pool.getHost(), op.execute(null, null),
                    think(5));
        }

        @Override
        public void open(long timeout) throws ConnectionException {
        }
    },

    GOOD_IMMEDIATE {

        @Override
        public <R> OperationResult<R> execute(
                HostConnectionPool<TestClient> pool, Operation<TestClient, R> op)
                throws ConnectionException {
            return new OperationResultImpl<R>(pool.getHost(), op.execute(null, null),
                    0);
        }

        @Override
        public void open(long timeout) throws ConnectionException {
        }
    },

    FAIL_AFTER_100 {
        private AtomicInteger counter = new AtomicInteger(0);

        @Override
        public <R> OperationResult<R> execute(
                HostConnectionPool<TestClient> pool, Operation<TestClient, R> op)
                throws ConnectionException {
            if (counter.incrementAndGet() > 100) {
                counter.set(0);
                throw new TransportException("TransportException");
            }
            return new OperationResultImpl<R>(pool.getHost(), op.execute(null, null),
                    0);
        }

        @Override
        public void open(long timeout) throws ConnectionException {
        }
    },

    FAIL_AFTER_100_RANDOM {
        private AtomicInteger counter = new AtomicInteger(0);

        @Override
        public <R> OperationResult<R> execute(
                HostConnectionPool<TestClient> pool, Operation<TestClient, R> op)
                throws ConnectionException {
            if (counter.incrementAndGet() > new Random().nextInt(1000)) {
                counter.set(0);
                throw new TransportException("TransportException");
            }
            return new OperationResultImpl<R>(pool.getHost(), op.execute(null, null),
                    0);
        }

        @Override
        public void open(long timeout) throws ConnectionException {
        }
    },

    THRASHING_TIMEOUT {

        @Override
        public <R> OperationResult<R> execute(
                HostConnectionPool<TestClient> pool, Operation<TestClient, R> op)
                throws ConnectionException {
            think(50 + new Random().nextInt(1000));
            throw new TimeoutException("thrashing_timeout");
        }

        @Override
        public void open(long timeout) throws ConnectionException {
        }
    },

    RECONNECT_2ND_TRY {
        int retry = 0;

        @Override
        public <R> OperationResult<R> execute(
                HostConnectionPool<TestClient> pool, Operation<TestClient, R> op)
                throws ConnectionException {
            throw new TransportException("TransportException");
        }

        @Override
        public void open(long timeout) throws ConnectionException {
            if (++retry == 2) {
                // success
            } else {
                throw new TransportException("TransportException");
            }
        }
    },

    ABORTED_CONNECTION {
        boolean aborted = true;

        @Override
        public <R> OperationResult<R> execute(
                HostConnectionPool<TestClient> pool, Operation<TestClient, R> op)
                throws ConnectionException {
            if (aborted) {
                aborted = false;
                throw new ConnectionAbortedException(
                        "ConnectionAbortedException");
            }
            return new OperationResultImpl<R>(pool.getHost(), op.execute(null, null),
                    0);
        }

        @Override
        public void open(long timeout) throws ConnectionException {
        }
    },

    FAIL_AFTER_10_SLOW_CLOSE {
        private AtomicInteger counter = new AtomicInteger(0);

        @Override
        public <R> OperationResult<R> execute(
                HostConnectionPool<TestClient> pool, Operation<TestClient, R> op)
                throws ConnectionException {
            think(100);
            if (counter.incrementAndGet() > 10) {
                counter.set(0);
                throw new TransportException("TransportException");
            }
            return new OperationResultImpl<R>(pool.getHost(), op.execute(null, null),
                    0);
        }

        @Override
        public void open(long timeout) throws ConnectionException {
        }

        @Override
        public void close() {
//            LOG.info("Closing");
            think(15000);
        }
    },
    
    SWAP_EVERY_200 {
        private AtomicInteger counter = new AtomicInteger(0);

        @Override
        public <R> OperationResult<R> execute(
                HostConnectionPool<TestClient> pool, Operation<TestClient, R> op)
                throws ConnectionException {
            if ((counter.incrementAndGet() / 20) % 2 == 0) {
                think(100);
            }
            else {
                think(1);
            }
            
            return new OperationResultImpl<R>(pool.getHost(), op.execute(null, null),
                    0);
        }

        @Override
        public void open(long timeout) throws ConnectionException {
        }

        @Override
        public void close() {
//            LOG.info("Closing");
            think(15000);
        }
    },

    RANDOM_ALL {
        @Override
        public <R> OperationResult<R> execute(
                HostConnectionPool<TestClient> pool, Operation<TestClient, R> op)
                throws ConnectionException {
            return new OperationResultImpl<R>(pool.getHost(), op.execute(null, null),
                    0);
        }

        @Override
        public void open(long timeout) throws ConnectionException {
            double p = new Random().nextDouble();
            if (p < 0.002) {
                throw new HostDownException("HostDownException");
            } else if (p < 0.004) {
                throw new TimeoutException("HostDownException");
            } else if (p < 0.006) {
                throw new TransportException("TransportException");
            }
            think(200);
        }

        @Override
        public void close() {
            // LOG.info("Closing");
            think(10);
        }
    };

    private static final Logger LOG = LoggerFactory
            .getLogger(TestHostType.class);

    private static final Map<Integer, TestHostType> lookup = new HashMap<Integer, TestHostType>();

    static {
        for (TestHostType type : EnumSet.allOf(TestHostType.class))
            lookup.put(type.ordinal(), type);
    }

    public static TestHostType get(int ordinal) {
        return lookup.get(ordinal);
    }

    public abstract <R> OperationResult<R> execute(
            HostConnectionPool<TestClient> pool, Operation<TestClient, R> op)
            throws ConnectionException;

    public abstract void open(long timeout) throws ConnectionException;

    public void close() {
    }

    private static int think(int time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
        }
        return time;
    }

}
