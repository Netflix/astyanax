package com.netflix.astyanax.connectionpool.impl;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.HostStats;
import com.netflix.astyanax.connectionpool.exceptions.PoolTimeoutException;
import com.netflix.astyanax.connectionpool.exceptions.TimeoutException;
import com.netflix.astyanax.connectionpool.exceptions.BadRequestException;
import com.netflix.astyanax.connectionpool.exceptions.NoAvailableHostsException;
import com.netflix.astyanax.connectionpool.exceptions.OperationTimeoutException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;

public class CountingConnectionPoolMonitor implements ConnectionPoolMonitor {
    private AtomicLong operationFailureCount  = new AtomicLong();
    private AtomicLong operationSuccessCount  = new AtomicLong();
    private AtomicLong connectionCreateCount  = new AtomicLong();
    private AtomicLong connectionClosedCount  = new AtomicLong();
    private AtomicLong connectionCreateFailureCount = new AtomicLong();
    private AtomicLong connectionBorrowCount  = new AtomicLong();
    private AtomicLong connectionReturnCount  = new AtomicLong();
    
    private AtomicLong operationFailoverCount = new AtomicLong();
    
    private AtomicLong hostAddedCount         = new AtomicLong();
    private AtomicLong hostRemovedCount       = new AtomicLong();
    private AtomicLong hostDownCount          = new AtomicLong();
    private AtomicLong hostReactivatedCount   = new AtomicLong();
    
    private AtomicLong poolExhastedCount      = new AtomicLong();
    private AtomicLong operationTimeoutCount  = new AtomicLong();
    private AtomicLong socketTimeoutCount     = new AtomicLong();
    private AtomicLong noHostsCount           = new AtomicLong();
    private AtomicLong unknownErrorCount      = new AtomicLong();
    private AtomicLong badRequestCount        = new AtomicLong();

    private AtomicLong notFoundCounter        = new AtomicLong();
    
    public CountingConnectionPoolMonitor() {
    }
    
    private void trackError(Host host, Exception reason) {
        if (reason instanceof PoolTimeoutException) {
            this.poolExhastedCount.incrementAndGet();
        }
        else if (reason instanceof TimeoutException) {
            this.socketTimeoutCount.incrementAndGet();
        }
        else if (reason instanceof OperationTimeoutException) {
            this.operationTimeoutCount.incrementAndGet();
        }
        else if (reason instanceof BadRequestException) {
            this.badRequestCount.incrementAndGet();
        }
        else if (reason instanceof NoAvailableHostsException ) {
            this.noHostsCount.incrementAndGet();
        }
        else {
            this.unknownErrorCount.incrementAndGet();
        }
    }

    @Override
    public void incOperationFailure(Host host, Exception reason) {
        if (reason instanceof NotFoundException) {
            this.notFoundCounter.incrementAndGet();
            return;
        }
        
        this.operationFailureCount.incrementAndGet();
        trackError(host, reason);
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

    public long getPoolExhaustedTimeoutCount() {
        return this.poolExhastedCount.get();
    }

    @Override
    public long getSocketTimeoutCount() {
        return this.socketTimeoutCount.get();
    }
    
    public long getOperationTimeoutCount() {
        return this.operationTimeoutCount.get();
    }

    @Override
    public void incFailover(Host host, Exception reason) {
        this.operationFailoverCount.incrementAndGet();
        trackError(host, reason);
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
    public long getNoHostCount() {
        return this.noHostsCount.get();
    }

    @Override
    public long getUnknownErrorCount() {
        return this.unknownErrorCount.get();
    }

    @Override
    public long getBadRequestCount() {
        return this.badRequestCount.get();
    }

    public long getNumBusyConnections() {
        return this.connectionBorrowCount.get() - this.connectionReturnCount.get();
    }

    public long getNumOpenConnections() {
        return this.connectionCreateCount.get() - this.connectionClosedCount.get();
    }
    
    @Override
    public long notFoundCount() {
        return this.notFoundCounter.get();
    }


    public String toString() {
        // Build the complete status string
        return new StringBuilder().append("CountingConnectionPoolMonitor(").append("Connections[").append("open=")
                .append(getNumOpenConnections()).append(",busy=").append(getNumBusyConnections()).append(",create=")
                .append(connectionCreateCount.get()).append(",close=").append(connectionClosedCount.get())
                .append(",borrow=").append(connectionBorrowCount.get()).append(",return=")
                .append(connectionReturnCount.get()).append("], Operations[").append("success=")
                .append(operationSuccessCount.get()).append(",failure=").append(operationFailureCount.get())
                .append(",timeout=").append(operationTimeoutCount.get()).append(",failover=")
                .append(operationFailoverCount.get()).append(",nohosts=").append(noHostsCount.get())
                .append("], Hosts[").append("add=").append(hostAddedCount.get()).append(",remove=")
                .append(hostRemovedCount.get()).append(",down=").append(hostDownCount.get()).append(",reactivate=")
                .append(hostReactivatedCount.get()).append("])").toString();
    }

    @Override
    public Map<Host, HostStats> getHostStats() {
        throw new UnsupportedOperationException("Not supported");
    }
}
