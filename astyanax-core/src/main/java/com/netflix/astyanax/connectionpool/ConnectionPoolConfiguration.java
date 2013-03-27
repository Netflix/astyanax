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

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import com.netflix.astyanax.AuthenticationCredentials;
import com.netflix.astyanax.connectionpool.impl.HostSelectorStrategy;
import com.netflix.astyanax.partitioner.Partitioner;

public interface ConnectionPoolConfiguration {
    /**
     * TODO
     */
    LatencyScoreStrategy getLatencyScoreStrategy();

    /**
     * TODO
     */
    BadHostDetector getBadHostDetector();

    /**
     * @return Data port to be used when no port is specified to a list of seeds or when
     * doing a ring describe since the ring describe does not include a host
     */
    int getPort();

    /**
     * @return Unique name assigned to this connection pool
     */
    String getName();

    /**
     * @return Maximum number of connections to allocate for a single host's pool
     */
    int getMaxConnsPerHost();

    /**
     * @return Initial number of connections created when a connection pool is started
     */
    int getInitConnsPerHost();

    /**
     * @return Maximum number of connections in the pool, not used by all connection
     * pool implementations
     */
    int getMaxConns();

    /**
     * @return Maximum amount of time to wait for a connection to free up when a
     * connection pool is exhausted.
     * 
     * @return
     */
    int getMaxTimeoutWhenExhausted();

    /**
     * @return Get the max number of failover attempts
     */
    int getMaxFailoverCount();

    /**
     * @return Return the backoff strategy to use.
     * 
     * @see com.netflix.astyanax.connectionpool.RetryBackoffStrategy
     */
    RetryBackoffStrategy getRetryBackoffStrategy();

    /**
     * @return Return the host selector strategy to use.
     *
     * @see com.netflix.astyanax.connectionpool.impl.HostSelectorStrategy
     */
    HostSelectorStrategy getHostSelectorStrategy();

    /**
     * @return List of comma delimited host:port combinations. If port is not provided
     * then getPort() will be used by default. This list must contain at least
     * one valid host other it would not be possible to do a ring describe.
     */
    String getSeeds();

    /**
     * @return  Return a list of Host objects from the list of seeds returned by
     * getSeeds(). This list must contain at least one valid host other it would
     * not be possible to do a ring describe.
     */
    List<Host> getSeedHosts();

    /**
     * @return Return local datacenter name.
     */
    public String getLocalDatacenter();

    /**
     * @return Socket read/write timeout
     */
    int getSocketTimeout();

    /**
     * @return Socket connect timeout
     */
    int getConnectTimeout();

    /**
     * @return Window size for limiting the number of connection open requests
     */
    int getConnectionLimiterWindowSize();

    /**
     * @return Maximum number of connection attempts in a given window
     */
    int getConnectionLimiterMaxPendingCount();

    /**
     * @return Latency samples window size for scoring algorithm
     */
    int getLatencyAwareWindowSize();

    /**
     * @return Sentinel compare value for Phi Accrual
     */
    float getLatencyAwareSentinelCompare();

    /**
     * @return  Return the threshold after which a host will not be considered good
     * enough for executing operations.
     */
    float getLatencyAwareBadnessThreshold();

    /**
     * @return  Return the threshold for disabling hosts that have nBlockedThreads more than
     * the least blocked host.  The idea here is to quarantine hosts that are slow to respond
     * and free up connections.
     */
    int getBlockedThreadThreshold();
    
    /**
     * @return Return the ratio for keeping a minimum number of hosts in the pool even if they are slow
     * or are blocked.  For example, a ratio of 0.75 with a connection pool of 12 hosts will 
     * ensure that no more than 4 hosts can be quaratined.
     */
    float getMinHostInPoolRatio();
    
    /**
     * TODO
     */
    int getLatencyAwareUpdateInterval();

    /**
     * TODO
     */
    int getLatencyAwareResetInterval();

    /**
     * @return Maximum number of pending connect attempts per host
     */
    int getMaxPendingConnectionsPerHost();

    /**
     * @return Get max number of blocked clients for a host.
     */
    int getMaxBlockedThreadsPerHost();

    /**
     * @return  Shut down a host if it times out too many time within this window
     */
    int getTimeoutWindow();

    /**
     * @return Number of allowed timeouts within getTimeoutWindow() milliseconds
     */
    int getMaxTimeoutCount();

    /**
     * TODO
     */
    int getRetrySuspendWindow();

    /**
     * TODO
     */
    int getRetryMaxDelaySlice();

    /**
     * TODO
     */
    int getRetryDelaySlice();

    /**
     * @return  Maximum allowed operations per connections before forcing the connection
     * to close
     */
    int getMaxOperationsPerConnection();

    /**
     * @return Can return null if no login required
     */
    AuthenticationCredentials getAuthenticationCredentials();
    
    /**
     * @return Return factory that will wrap an operation with filters, such as logging filters
     * and simulation filters
     */
    OperationFilterFactory getOperationFilterFactory();
    
    /**
     * @return Get the partitioner used to convert row keys to TOKEN
     */
    Partitioner getPartitioner();

    /**
     * @return Retrieve a context to determine if connections should be made using SSL.
     */
    SSLConnectionContext getSSLConnectionContext();

    /**
     * @return Return executor service used for maintenance tasks.  This pool is used for internal
     * operations that update stats such as token aware scores.  Threads in this pool 
     * and not expected to block on I/O and the pool can therefore be very small
     */
    ScheduledExecutorService getMaintainanceScheduler();

    /**
     * @return Return executor service used to reconnect hosts.  Keep in mind that the threads 
     * may be blocked for an extended duration while trying to reconnect to a downed host
     */
    ScheduledExecutorService getHostReconnectExecutor();

    /**
     * Initialization prior to starting the connection pool 
     */
    void initialize();

    /**
     * Shutdown after stopping the connection pool
     */
    void shutdown();
}
