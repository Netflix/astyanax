package com.netflix.astyanax.connectionpool.impl;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.HostStats;

public class CountingConnectionPoolMonitor implements ConnectionPoolMonitor {
    private AtomicLong operationFailureCount = new AtomicLong();
    private AtomicLong operationSuccessCount = new AtomicLong();
    private AtomicLong connectionCreateCount = new AtomicLong();
    private AtomicLong connectionClosedCount = new AtomicLong();
    private AtomicLong connectionCreateFailureCount = new AtomicLong();
    private AtomicLong connectionBorrowCount = new AtomicLong();
    private AtomicLong connectionReturnCount = new AtomicLong();
    private AtomicLong poolExhastedCount = new AtomicLong();
    private AtomicLong operationTimeoutCount = new AtomicLong();
    private AtomicLong operationFailoverCount = new AtomicLong();
    private AtomicLong hostAddedCount = new AtomicLong();
    private AtomicLong hostRemovedCount = new AtomicLong();
    private AtomicLong hostDownCount = new AtomicLong();
    private AtomicLong hostReactivatedCount = new AtomicLong();
    private AtomicLong noHostsCount = new AtomicLong();

    public CountingConnectionPoolMonitor() {
    }

    @Override
    public void incOperationFailure(Host host, Exception reason) {
        this.operationFailureCount.incrementAndGet();
    }

    public long getOperationFailureCount() {
        return this.operationFailureCount.get();
    }

    @Override
    public void incOperationSuccess(Host host, long latency) {
        this.operationSuccessCount.incrementAndGet();
    }

    public long getOperationSuccessCount() {
        return this.operationSuccessCount.get();
    }

    @Override
    public void incConnectionCreated(Host host) {
        this.connectionCreateCount.incrementAndGet();
    }

    public long getConnectionCreatedCount() {
        return this.connectionCreateCount.get();
    }

    @Override
    public void incConnectionClosed(Host host, Exception reason) {
        this.connectionClosedCount.incrementAndGet();
    }

    public long getConnectionClosedCount() {
        return this.connectionClosedCount.get();
    }

    @Override
    public void incConnectionCreateFailed(Host host, Exception reason) {
        this.connectionCreateFailureCount.incrementAndGet();
    }

    public long getConnectionCreateFailedCount() {
        return this.connectionCreateFailureCount.get();
    }

    @Override
    public void incConnectionBorrowed(Host host, long delay) {
        this.connectionBorrowCount.incrementAndGet();
    }

    public long getConnectionBorrowedCount() {
        return this.connectionBorrowCount.get();
    }

    @Override
    public void incConnectionReturned(Host host) {
        this.connectionReturnCount.incrementAndGet();
    }

    public long getConnectionReturnedCount() {
        return this.connectionReturnCount.get();
    }

    @Override
    public void incPoolExhaustedTimeout() {
        this.poolExhastedCount.incrementAndGet();
    }

    public long getPoolExhaustedTimeoutCount() {
        return this.poolExhastedCount.get();
    }

    @Override
    public void incOperationTimeout() {
        this.operationTimeoutCount.incrementAndGet();
    }

    public long getOperationTimeoutCount() {
        return this.operationTimeoutCount.get();
    }

    @Override
    public void incFailover(Host host, Exception reason) {
        this.operationFailoverCount.incrementAndGet();
    }

    public long getFailoverCount() {
        return this.operationFailoverCount.get();
    }

    @Override
    public void onHostAdded(Host host, HostConnectionPool<?> pool) {
        this.hostAddedCount.incrementAndGet();
    }

    public long getHostAddedCount() {
        return this.hostAddedCount.get();
    }

    @Override
    public void onHostRemoved(Host host) {
        this.hostRemovedCount.incrementAndGet();
    }

    public long getHostRemovedCount() {
        return this.hostRemovedCount.get();
    }

    @Override
    public void onHostDown(Host host, Exception reason) {
        this.hostDownCount.incrementAndGet();
    }

    public long getHostDownCount() {
        return this.hostDownCount.get();
    }

    @Override
    public void onHostReactivated(Host host, HostConnectionPool<?> pool) {
        this.hostReactivatedCount.incrementAndGet();
    }

    public long getHostReactivatedCount() {
        return this.hostReactivatedCount.get();
    }

    @Override
    public void incNoHosts() {
        this.noHostsCount.incrementAndGet();
        this.operationFailoverCount.incrementAndGet();
    }

    public long getNoHostsCount() {
        return this.noHostsCount.get();
    }

    public long getNumBusyConnections() {
        return this.connectionBorrowCount.get()
                - this.connectionReturnCount.get();
    }

    public long getNumOpenConnections() {
        return this.connectionCreateCount.get()
                - this.connectionClosedCount.get();
    }

    public String toString() {
        // Build the complete status string
        return new StringBuilder().append("CountingConnectionPoolMonitor(")
                .append("Connections[").append("open=")
                .append(getNumOpenConnections()).append(",busy=")
                .append(getNumBusyConnections()).append(",create=")
                .append(connectionCreateCount.get()).append(",close=")
                .append(connectionClosedCount.get()).append(",borrow=")
                .append(connectionBorrowCount.get()).append(",return=")
                .append(connectionReturnCount.get()).append("], Operations[")
                .append("success=").append(operationSuccessCount.get())
                .append(",failure=").append(operationFailureCount.get())
                .append(",timeout=").append(operationTimeoutCount.get())
                .append(",failover=").append(operationFailoverCount.get())
                .append(",nohosts=").append(noHostsCount.get())
                .append("], Hosts[").append("add=")
                .append(hostAddedCount.get()).append(",remove=")
                .append(hostRemovedCount.get()).append(",down=")
                .append(hostDownCount.get()).append(",reactivate=")
                .append(hostReactivatedCount.get()).append("])").toString();
    }

    @Override
    public Map<Host, HostStats> getHostStats() {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public long getNoHostCount() {
        return this.noHostsCount.get();
    }

}
