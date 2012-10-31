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

import com.netflix.astyanax.AuthenticationCredentials;
import com.netflix.astyanax.partitioner.Partitioner;

public interface ConnectionPoolConfiguration {
    /**
     * TODO
     * 
     * @return
     */
    LatencyScoreStrategy getLatencyScoreStrategy();

    /**
     * TODO
     * 
     * @return
     */
    BadHostDetector getBadHostDetector();

    /**
     * Data port to be used when no port is specified to a list of seeds or when
     * doing a ring describe since the ring describe does not include a host
     * 
     * @return
     */
    int getPort();

    /**
     * Unique name assigned to this connection pool
     * 
     * @return
     */
    String getName();

    /**
     * Maximum number of connections to allocate for a single host's pool
     * 
     * @return
     */
    int getMaxConnsPerHost();

    /**
     * Initial number of connections created when a connection pool is started
     * 
     * @return
     */
    int getInitConnsPerHost();

    /**
     * Maximum number of connections in the pool, not used by all connection
     * pool implementations
     * 
     * @return
     */
    int getMaxConns();

    /**
     * Maximum amount of time to wait for a connection to free up when a
     * connection pool is exhausted.
     * 
     * @return
     */
    int getMaxTimeoutWhenExhausted();

    /**
     * Get the max number of failover attempts
     * 
     * @return
     */
    int getMaxFailoverCount();

    /**
     * Return the backoff strategy to use.
     * 
     * @see com.netflix.astyanax.connectionpool.RetryBackoffStrategy
     * @return
     */
    RetryBackoffStrategy getRetryBackoffStrategy();

    /**
     * List of comma delimited host:port combinations. If port is not provided
     * then getPort() will be used by default. This list must contain at least
     * one valid host other it would not be possible to do a ring describe.
     * 
     * @return
     */
    String getSeeds();

    /**
     * Return a list of Host objects from the list of seeds returned by
     * getSeeds(). This list must contain at least one valid host other it would
     * not be possible to do a ring describe.
     * 
     * @return
     */
    List<Host> getSeedHosts();

    /**
     * Return local datacenter name.
     * 
     * @return
     */
    public String getLocalDatacenter();

    /**
     * Socket read/write timeout
     * 
     * @return
     */
    int getSocketTimeout();

    /**
     * Socket connect timeout
     * 
     * @return
     */
    int getConnectTimeout();

    /**
     * Window size for limiting the number of connection open requests
     * 
     * @return
     */
    int getConnectionLimiterWindowSize();

    /**
     * Maximum number of connection attempts in a given window
     * 
     * @return
     */
    int getConnectionLimiterMaxPendingCount();

    /**
     * Latency samples window size for scoring algorithm
     * 
     * @return
     */
    int getLatencyAwareWindowSize();

    /**
     * Sentinel compare value for Phi Accrual
     * 
     * @return
     */
    float getLatencyAwareSentinelCompare();

    /**
     * Return the threshold after which a host will not be considered good
     * enough for executing operations.
     * 
     * @return Valid values are 0 to 1
     */
    float getLatencyAwareBadnessThreshold();

    /**
     * Return the threshold for disabling hosts that have nBlockedThreads more than
     * the least blocked host.  The idea here is to quarantine hosts that are slow to respond
     * and free up connections.
     * 
     * @return
     */
    int getBlockedThreadThreshold();
    
    /**
     * Return the ratio for keeping a minimum number of hosts in the pool even if they are slow
     * or are blocked.  For example, a ratio of 0.75 with a connection pool of 12 hosts will 
     * ensure that no more than 4 hosts can be quaratined.
     * 
     * @return
     */
    float getMinHostInPoolRatio();
    
    /**
     * 
     * @return
     */
    int getLatencyAwareUpdateInterval();

    /**
     * 
     * @return
     */
    int getLatencyAwareResetInterval();

    /**
     * Maximum number of pending connect attempts per host
     * 
     * @return
     */
    int getMaxPendingConnectionsPerHost();

    /**
     * Get max number of blocked clients for a host.
     * 
     * @return
     */
    int getMaxBlockedThreadsPerHost();

    /**
     * Shut down a host if it times out too many time within this window
     * 
     * @return
     */
    int getTimeoutWindow();

    /**
     * Number of allowed timeouts within getTimeoutWindow() milliseconds
     * 
     * @return
     */
    int getMaxTimeoutCount();

    /**
     * TODO
     * 
     * @return
     */
    int getRetrySuspendWindow();

    /**
     * TODO
     * 
     * @return
     */
    int getRetryMaxDelaySlice();

    /**
     * TODO
     * 
     * @return
     */
    int getRetryDelaySlice();

    /**
     * Maximum allowed operations per connections before forcing the connection
     * to close
     * 
     * @return
     */
    int getMaxOperationsPerConnection();

    /**
     * Can return null if no login required
     * 
     * @return
     */
    AuthenticationCredentials getAuthenticationCredentials();
    
    /**
     * Return factory that will wrap an operation with filters, such as logging filters
     * and simulation filters
     * @return
     */
    OperationFilterFactory getOperationFilterFactory();
    
    /**
     * Get the partitioner used to convert row keys to TOKEN
     * @return
     */
    Partitioner getPartitioner();
}
