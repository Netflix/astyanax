package com.netflix.astyanax.connectionpool.impl;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.netflix.astyanax.connectionpool.exceptions.HostDownException;
import com.netflix.astyanax.connectionpool.exceptions.TransportException;
import com.netflix.astyanax.connectionpool.exceptions.InterruptedOperationException;

/**
 * 
 * Impl for {@link ConnectionPoolMonitor} that employs counters to track stats such as 
 * 
 * <ol>
 * <li> operation success / failures / timeouts / socket timeouts / interrupted </li>
 * <li> connection created /  borrowed / returned / closed / create failures </li>
 * <li> hosts added /  removed / marked as down / reactivated </li>
 * <li> transport failures and other useful stats </li>
 * </ol>
 * 
 * @author elandau
 */
public class CountingConnectionPoolMonitor implements ConnectionPoolMonitor {
    private static Logger LOG = LoggerFactory.getLogger(CountingConnectionPoolMonitor.class);
    
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
    private AtomicLong interruptedCount       = new AtomicLong();
    private AtomicLong transportErrorCount    = new AtomicLong();

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
        else if (reason instanceof InterruptedOperationException) {
            this.interruptedCount.incrementAndGet();
        }
        else if (reason instanceof HostDownException) {
            this.hostDownCount.incrementAndGet();
        }
        else if (reason instanceof TransportException) {
            this.transportErrorCount.incrementAndGet();
        }
        else {
            LOG.error(reason.toString(), reason);
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

    @Override
    public long getFailoverCount() {
        return this.operationFailoverCount.get();
    }

    @Override
    public void onHostAdded(Host host, HostConnectionPool<?> pool) {
        LOG.info("AddHost: " + host.getHostName());
        this.hostAddedCount.incrementAndGet();
    }

    @Override
    public long getHostAddedCount() {
        return this.hostAddedCount.get();
    }

    @Override
    public void onHostRemoved(Host host) {
        LOG.info("RemoveHost: " + host.getHostName());
        this.hostRemovedCount.incrementAndGet();
    }

    @Override
    public long getHostRemovedCount() {
        return this.hostRemovedCount.get();
    }

    @Override
    public void onHostDown(Host host, Exception reason) {
        this.hostDownCount.incrementAndGet();
    }

    @Override
    public long getHostDownCount() {
        return this.hostDownCount.get();
    }

    @Override
    public void onHostReactivated(Host host, HostConnectionPool<?> pool) {
        LOG.info("Reactivating " + host.getHostName());
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
    public long getInterruptedCount() {
        return this.interruptedCount.get();
    }

    @Override
    public long getTransportErrorCount() {
        return this.transportErrorCount.get();
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

    @Override
    public long getHostCount() {
        return getHostAddedCount() - getHostRemovedCount();
    }

    @Override
    public long getHostActiveCount() {
        return hostAddedCount.get() - hostRemovedCount.get() + hostReactivatedCount.get() - hostDownCount.get();
    }

    public String toString() {
        // Build the complete status string
        return new StringBuilder()
                .append("CountingConnectionPoolMonitor(")
                .append("Connections[" )
                    .append( "open="       ).append(getNumOpenConnections())
                    .append(",busy="       ).append(getNumBusyConnections())
                    .append(",create="     ).append(connectionCreateCount.get())
                    .append(",close="      ).append(connectionClosedCount.get())
                    .append(",failed="     ).append(connectionCreateFailureCount.get())
                    .append(",borrow="     ).append(connectionBorrowCount.get())
                    .append(",return="     ).append(connectionReturnCount.get())
                .append("], Operations[")
                    .append( "success="    ).append(operationSuccessCount.get())
                    .append(",failure="    ).append(operationFailureCount.get())
                    .append(",optimeout="  ).append(operationTimeoutCount.get())
                    .append(",timeout="    ).append(socketTimeoutCount.get())
                    .append(",failover="   ).append(operationFailoverCount.get())
                    .append(",nohosts="    ).append(noHostsCount.get())
                    .append(",unknown="    ).append(unknownErrorCount.get())
                    .append(",interrupted=").append(interruptedCount.get())
                    .append(",exhausted="  ).append(poolExhastedCount.get())
                    .append(",transport="  ).append(transportErrorCount.get())
                .append("], Hosts[")
                    .append( "add="        ).append(hostAddedCount.get())
                    .append(",remove="     ).append(hostRemovedCount.get())
                    .append(",down="       ).append(hostDownCount.get())
                    .append(",reactivate=" ).append(hostReactivatedCount.get())
                    .append(",active="     ).append(getHostActiveCount())
                .append("])").toString();
    }

    @Override
    public Map<Host, HostStats> getHostStats() {
        throw new UnsupportedOperationException("Not supported");
    }
}
