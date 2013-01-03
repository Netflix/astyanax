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

import java.util.concurrent.atomic.AtomicBoolean;

import com.netflix.astyanax.connectionpool.Connection;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

public class TestHostConnectionPool implements HostConnectionPool<TestClient> {

    private final Host host;
    private AtomicBoolean isShutDown = new AtomicBoolean();

    public TestHostConnectionPool(Host host) {
        this.host = host;
    }

    @Override
    public Connection<TestClient> borrowConnection(int timeout)
            throws ConnectionException {
        return null;
    }

    @Override
    public boolean returnConnection(Connection<TestClient> connection) {
        return false;
    }

    @Override
    public void markAsDown(ConnectionException reason) {
        isShutDown.set(true);
    }

    @Override
    public void shutdown() {
        isShutDown.set(true);
    }

    @Override
    public int primeConnections(int numConnections) throws ConnectionException,
            InterruptedException {
        return 0;
    }

    @Override
    public Host getHost() {
        return host;
    }

    @Override
    public int getActiveConnectionCount() {
        return 0;
    }

    @Override
    public int getPendingConnectionCount() {
        return 0;
    }

    @Override
    public int getBlockedThreadCount() {
        return 0;
    }

    @Override
    public int getIdleConnectionCount() {
        return 0;
    }

    @Override
    public int getBusyConnectionCount() {
        return 0;
    }

    @Override
    public boolean isReconnecting() {
        return false;
    }

    @Override
    public double getScore() {
        return 0;
    }

    @Override
    public void addLatencySample(long lastLatency, long now) {

    }

    @Override
    public boolean closeConnection(Connection<TestClient> connection) {
        return false;
    }

    @Override
    public int getOpenedConnectionCount() {
        return 0;
    }

    @Override
    public int getFailedOpenConnectionCount() {
        return 0;
    }

    @Override
    public int getClosedConnectionCount() {
        return 0;
    }

    @Override
    public int getErrorsSinceLastSuccess() {
        return 0;
    }

    @Override
    public boolean isActive() {
        return false;
    }

    @Override
    public boolean isShutdown() {
        return false;
    }

    @Override
    public int getConnectAttemptCount() {
        return 0;
    }

}
