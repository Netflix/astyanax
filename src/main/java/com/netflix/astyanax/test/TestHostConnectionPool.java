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
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean returnConnection(Connection<TestClient> connection) {
        // TODO Auto-generated method stub
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
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Host getHost() {
        return host;
    }

    @Override
    public int getActiveConnectionCount() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getPendingConnectionCount() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getBlockedThreadCount() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getIdleConnectionCount() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getBusyConnectionCount() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean isReconnecting() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public double getScore() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void addLatencySample(long lastLatency, long now) {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean closeConnection(Connection<TestClient> connection) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int getOpenedConnectionCount() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getFailedOpenConnectionCount() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getClosedConnectionCount() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getErrorsSinceLastSuccess() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean isActive() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isShutdown() {
        // TODO Auto-generated method stub
        return false;
    }

}
