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
package com.netflix.astyanax.connectionpool;

import java.util.Map;

/**
 * Monitoring interface to receive notification of pool events. A concrete
 * monitor will make event stats available to a monitoring application and may
 * also log events to a log file.
 * 
 * @author elandau
 */
public interface ConnectionPoolMonitor {
    /**
     * Errors trying to execute an operation
     * 
     * @param reason
     * @param host
     */
    void incOperationFailure(Host host, Exception reason);

    long getOperationFailureCount();

    /**
     * Succeeded in executing an operation
     * 
     * @param host
     * @param latency
     */
    void incOperationSuccess(Host host, long latency);

    long getOperationSuccessCount();

    /**
     * Created a connection successfully
     */
    void incConnectionCreated(Host host);

    long getConnectionCreatedCount();

    /**
     * Closed a connection
     * 
     * @param reason
     *            TODO: Make the host available to this
     */
    void incConnectionClosed(Host host, Exception reason);

    long getConnectionClosedCount();

    /**
     * Attempt to create a connection failed
     * 
     * @param host
     * @param reason
     */
    void incConnectionCreateFailed(Host host, Exception reason);

    long getConnectionCreateFailedCount();

    /**
     * Incremented for each connection borrowed
     * 
     * @param host
     *            Host from which the connection was borrowed
     * @param delay
     *            Time spent in the connection pool borrowing the connection
     */
    void incConnectionBorrowed(Host host, long delay);

    long getConnectionBorrowedCount();

    /**
     * Incremented for each connection returned.
     * 
     * @param host
     *            Host to which connection is returned
     */
    void incConnectionReturned(Host host);

    long getConnectionReturnedCount();

    /**
     * Timeout trying to get a connection from the pool
     */
    void incPoolExhaustedTimeout();

    long getPoolExhaustedTimeoutCount();

    /**
     * Timeout waiting for a response from the cluster
     */
    void incOperationTimeout();

    long getOperationTimeoutCount();

    /**
     * An operation failed by the connection pool will attempt to fail over to
     * another host/connection.
     */
    void incFailover(Host host, Exception reason);

    long getFailoverCount();

    /**
     * A host was added and given the associated pool. The pool is immutable and
     * can be used to get info about the number of open connections
     * 
     * @param host
     * @param pool
     */
    void onHostAdded(Host host, HostConnectionPool<?> pool);

    /**
     * A host was removed from the pool. This is usually called when a downed
     * host is removed from the ring.
     * 
     * @param host
     */
    void onHostRemoved(Host host);

    /**
     * A host was identified as downed.
     * 
     * @param host
     * @param reason
     *            Exception that caused the host to be identified as down
     */
    void onHostDown(Host host, Exception reason);

    /**
     * A host was reactivated after being marked down
     * 
     * @param host
     * @param pool
     */
    void onHostReactivated(Host host, HostConnectionPool<?> pool);

    /**
     * There were no active hosts in the pool to borrow from.
     */
    void incNoHosts();

    long getNoHostCount();

    /**
     * Return a mapping of all hosts and their statistics
     * 
     * @return
     */
    Map<Host, HostStats> getHostStats();
}
