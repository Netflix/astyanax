package com.netflix.astyanax.fake;

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
    public Connection<TestClient> openConnection() throws ConnectionException {
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
    public int growConnections(int numConnections) throws ConnectionException,
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
    public boolean isShutdown() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public double getScore() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public double getMeanLatency() {
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

}
