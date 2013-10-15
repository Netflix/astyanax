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
package com.netflix.astyanax.connectionpool.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.astyanax.AuthenticationCredentials;
import com.netflix.astyanax.connectionpool.BadHostDetector;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.LatencyScoreStrategy;
import com.netflix.astyanax.connectionpool.OperationFilterFactory;
import com.netflix.astyanax.connectionpool.RetryBackoffStrategy;
import com.netflix.astyanax.connectionpool.SSLConnectionContext;
import com.netflix.astyanax.partitioner.Partitioner;
import com.netflix.astyanax.shallows.EmptyBadHostDetectorImpl;
import com.netflix.astyanax.shallows.EmptyLatencyScoreStrategyImpl;
import com.netflix.astyanax.shallows.EmptyOperationFilterFactory;

/**
 * 
 * Basic impl for {@link ConnectionPoolConfiguration} that uses a bunch of defaults for al the connection pool config. 
 * 
 * @author elandau
 */
public class ConnectionPoolConfigurationImpl implements ConnectionPoolConfiguration {
    /**
     * Default values
     */
    public static final int DEFAULT_MAX_TIME_WHEN_EXHAUSTED = 2000;
    public static final int DEFAULT_SOCKET_TIMEOUT = 11000;	// ms
    public static final int DEFAULT_CONNECT_TIMEOUT = 2000; // ms
    public static final int DEFAULT_MAX_ACTIVE_PER_PARTITION = 3;
    public static final int DEFAULT_INIT_PER_PARTITION = 0;
    public static final int DEFAULT_PORT = 9160;
    public static final int DEFAULT_FAILOVER_COUNT = -1;
    public static final int DEFAULT_MAX_CONNS = 1;
    public static final int DEFAULT_LATENCY_AWARE_WINDOW_SIZE = 100;
    public static final float DEFAULT_LATENCY_AWARE_SENTINEL_COMPARE = 0.768f;
    public static final int DEFAULT_LATENCY_AWARE_UPDATE_INTERVAL = 10000;
    public static final int DEFAULT_LATENCY_AWARE_RESET_INTERVAL = 60000;
    public static final float DEFAULT_LATENCY_AWARE_BADNESS_THRESHOLD = 0.10f;
    public static final int DEFAULT_CONNECTION_LIMITER_WINDOW_SIZE = 2000;
    public static final int DEFAULT_CONNECTION_LIMITER_MAX_PENDING_COUNT = 50;
    public static final int DEFAULT_MAX_PENDING_CONNECTIONS_PER_HOST = 5;
    public static final int DEFAULT_MAX_BLOCKED_THREADS_PER_HOST = 25;
    public static final int DEFAULT_MAX_TIMEOUT_COUNT = 3;
    public static final int DEFAULT_TIMEOUT_WINDOW = 10000;
    public static final int DEFAULT_RETRY_SUSPEND_WINDOW = 20000;
    public static final int DEFAULT_RETRY_DELAY_SLICE = 10000;
    public static final int DEFAULT_RETRY_MAX_DELAY_SLICE = 10;
    public static final int DEFAULT_MAX_OPERATIONS_PER_CONNECTION = 10000;
    public static final float DEFAULT_MIN_HOST_IN_POOL_RATIO = 0.65f;
    public static final int DEFAULT_BLOCKED_THREAD_THRESHOLD = 10;
    public static final BadHostDetector DEFAULT_BAD_HOST_DETECTOR = EmptyBadHostDetectorImpl.getInstance();
//    public static final Partitioner DEFAULT_PARTITIONER = BigInteger127Partitioner.get();
    private static final int DEFAULT_RECONNECT_THREAD_COUNT = 5;
    private static final int DEFAULT_MAINTAINANCE_THREAD_COUNT = 1;
    
    private static final String DEFAULT_PARTITIONER_CLASS = "com.netflix.astyanax.partitioner.BigInteger127Partitioner";

    private final String name;

    private int maxConnsPerPartition             = DEFAULT_MAX_ACTIVE_PER_PARTITION;
    private int initConnsPerPartition            = DEFAULT_INIT_PER_PARTITION;
    private int maxConns                         = DEFAULT_MAX_CONNS;
    private int port                             = DEFAULT_PORT;
    private int socketTimeout                    = DEFAULT_SOCKET_TIMEOUT;
    private int connectTimeout                   = DEFAULT_CONNECT_TIMEOUT;
    private int maxFailoverCount                 = DEFAULT_FAILOVER_COUNT;
    private int latencyAwareWindowSize           = DEFAULT_LATENCY_AWARE_WINDOW_SIZE;
    private float latencyAwareSentinelCompare    = DEFAULT_LATENCY_AWARE_SENTINEL_COMPARE;
    private float latencyAwareBadnessThreshold   = DEFAULT_LATENCY_AWARE_BADNESS_THRESHOLD;
    private int latencyAwareUpdateInterval       = DEFAULT_LATENCY_AWARE_UPDATE_INTERVAL;
    private int latencyAwareResetInterval        = DEFAULT_LATENCY_AWARE_RESET_INTERVAL;
    private int connectionLimiterWindowSize      = DEFAULT_CONNECTION_LIMITER_WINDOW_SIZE;
    private int connectionLimiterMaxPendingCount = DEFAULT_CONNECTION_LIMITER_MAX_PENDING_COUNT;
    private int maxPendingConnectionsPerHost     = DEFAULT_MAX_PENDING_CONNECTIONS_PER_HOST;
    private int maxBlockedThreadsPerHost         = DEFAULT_MAX_BLOCKED_THREADS_PER_HOST;
    private int maxTimeoutCount                  = DEFAULT_MAX_TIMEOUT_COUNT;
    private int timeoutWindow                    = DEFAULT_TIMEOUT_WINDOW;
    private int retrySuspendWindow               = DEFAULT_RETRY_SUSPEND_WINDOW;
    private int retryDelaySlice                  = DEFAULT_RETRY_DELAY_SLICE;
    private int retryMaxDelaySlice               = DEFAULT_RETRY_MAX_DELAY_SLICE;
    private int maxOperationsPerConnection       = DEFAULT_MAX_OPERATIONS_PER_CONNECTION;
    private int maxTimeoutWhenExhausted          = DEFAULT_MAX_TIME_WHEN_EXHAUSTED;
    private float minHostInPoolRatio             = DEFAULT_MIN_HOST_IN_POOL_RATIO;
    private int blockedThreadThreshold           = DEFAULT_BLOCKED_THREAD_THRESHOLD;

    private String seeds = null;
    private RetryBackoffStrategy hostRetryBackoffStrategy = null;
    private HostSelectorStrategy hostSelectorStrategy     = HostSelectorStrategy.ROUND_ROBIN;
    private LatencyScoreStrategy latencyScoreStrategy     = new EmptyLatencyScoreStrategyImpl();
    private BadHostDetector badHostDetector               = DEFAULT_BAD_HOST_DETECTOR;
    private AuthenticationCredentials credentials         = null;
    private OperationFilterFactory filterFactory          = EmptyOperationFilterFactory.getInstance();
    private Partitioner partitioner                       = null;
    private SSLConnectionContext sslCtx;

    private ScheduledExecutorService maintainanceExecutor;
    private ScheduledExecutorService reconnectExecutor;
    
    private boolean bOwnMaintainanceExecutor              = false;
    private boolean bOwnReconnectExecutor                 = false;
            
    private String localDatacenter = null;

    public ConnectionPoolConfigurationImpl(String name) {
        this.name = name;
        this.badHostDetector = new BadHostDetectorImpl(this);
        this.hostRetryBackoffStrategy = new ExponentialRetryBackoffStrategy(this);
    }

    @Override
    public void initialize() {
        if (partitioner == null) {
            try {
                partitioner = (Partitioner) Class.forName(DEFAULT_PARTITIONER_CLASS).newInstance();
            } catch (Throwable t) {
                throw new RuntimeException("Can't instantiate default partitioner " + DEFAULT_PARTITIONER_CLASS, t);
            }
        }
        
        if (maintainanceExecutor == null) {
            maintainanceExecutor = Executors.newScheduledThreadPool(DEFAULT_MAINTAINANCE_THREAD_COUNT, new ThreadFactoryBuilder().setDaemon(true).build());
            bOwnMaintainanceExecutor = true;
        }
        if (reconnectExecutor == null) {
            reconnectExecutor = Executors.newScheduledThreadPool(DEFAULT_RECONNECT_THREAD_COUNT, new ThreadFactoryBuilder().setDaemon(true).build());
            bOwnReconnectExecutor = true;
        }
    }
    
    @Override
    public void shutdown() {
        if (bOwnMaintainanceExecutor) {
            maintainanceExecutor.shutdownNow();
        }
        
        if (bOwnReconnectExecutor) {
            reconnectExecutor.shutdownNow();
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.netflix.cassandra.ConnectionPoolConfiguration#getKeyspaceName()
     */
    @Override
    public String getName() {
        return this.name;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.netflix.cassandra.ConnectionPoolConfiguration#getSocketTimeout()
     */
    @Override
    public int getSocketTimeout() {
        return socketTimeout;
    }

    public ConnectionPoolConfigurationImpl setSocketTimeout(int socketTimeout) {
        this.socketTimeout = socketTimeout;
        return this;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.netflix.cassandra.ConnectionPoolConfiguration#getConnectTimeout()
     */
    @Override
    public int getConnectTimeout() {
        return connectTimeout;
    }

    public ConnectionPoolConfigurationImpl setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
        return this;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.netflix.cassandra.ConnectionPoolConfiguration#getSeeds()
     */
    @Override
    public String getSeeds() {
        return this.seeds;
    }

    public ConnectionPoolConfigurationImpl setSeeds(String seeds) {
        this.seeds = seeds;
        return this;
    }

    /**
     * Returns local datacenter name
     *
     * @return
     */
    public String getLocalDatacenter() {
        return localDatacenter;
    }

    public ConnectionPoolConfigurationImpl setLocalDatacenter(String localDatacenter) {
        this.localDatacenter = localDatacenter;
        return this;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.netflix.cassandra.ConnectionPoolConfiguration#getMaxTimeoutWhenExhausted
     * ()
     */
    @Override
    public int getMaxTimeoutWhenExhausted() {
        return this.maxTimeoutWhenExhausted;
    }

    public ConnectionPoolConfigurationImpl setMaxTimeoutWhenExhausted(int timeout) {
        this.maxTimeoutWhenExhausted = timeout;
        return this;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.netflix.cassandra.ConnectionPoolConfiguration#getPort()
     */
    @Override
    public int getPort() {
        return this.port;
    }

    public ConnectionPoolConfigurationImpl setPort(int port) {
        this.port = port;
        return this;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.netflix.cassandra.ConnectionPoolConfiguration#getMaxConnsPerHost()
     */
    @Override
    public int getMaxConnsPerHost() {
        return this.maxConnsPerPartition;
    }

    public ConnectionPoolConfigurationImpl setMaxConnsPerHost(int maxConns) {
        Preconditions.checkArgument(maxConns > 0, "maxConnsPerHost must be >0");
        this.maxConnsPerPartition = maxConns;
        return this;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.netflix.cassandra.ConnectionPoolConfiguration#getInitConnsPerHost()
     */
    @Override
    public int getInitConnsPerHost() {
        return this.initConnsPerPartition;
    }

    public ConnectionPoolConfigurationImpl setInitConnsPerHost(int initConns) {
        Preconditions.checkArgument(initConns >= 0, "initConnsPerHost must be >0");
        this.initConnsPerPartition = initConns;
        return this;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.netflix.cassandra.ConnectionPoolConfiguration#getRetryBackoffStrategy
     * ()
     */
    @Override
    public RetryBackoffStrategy getRetryBackoffStrategy() {
        return this.hostRetryBackoffStrategy;
    }

    public ConnectionPoolConfigurationImpl setRetryBackoffStrategy(RetryBackoffStrategy hostRetryBackoffStrategy) {
        this.hostRetryBackoffStrategy = hostRetryBackoffStrategy;
        return this;
    }

    @Override
    public HostSelectorStrategy getHostSelectorStrategy() {
        return this.hostSelectorStrategy;
    }

    public ConnectionPoolConfigurationImpl setHostSelectorStrategy(HostSelectorStrategy hostSelectorStrategy) {
        this.hostSelectorStrategy = hostSelectorStrategy;
        return this;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.netflix.cassandra.ConnectionPoolConfiguration#getSeedHosts()
     */
    @Override
    public List<Host> getSeedHosts() {
        List<Host> hosts = new ArrayList<Host>();
        if (seeds != null) {
            for (String seed : seeds.split(",")) {
                seed = seed.trim();
                if (seed.length() > 0) {
                    hosts.add(new Host(seed, this.port));
                }
            }
        }
        return hosts;
    }

    @Override
    public int getMaxFailoverCount() {
        return this.maxFailoverCount;
    }

    public ConnectionPoolConfigurationImpl setMaxFailoverCount(int maxFailoverCount) {
        this.maxFailoverCount = maxFailoverCount;
        return this;
    }

    @Override
    public int getMaxConns() {
        return this.maxConns;
    }

    public ConnectionPoolConfigurationImpl setMaxConns(int maxConns) {
        this.maxConns = maxConns;
        return this;
    }

    @Override
    public int getLatencyAwareWindowSize() {
        return this.latencyAwareWindowSize;
    }

    public ConnectionPoolConfigurationImpl setLatencyAwareWindowSize(int latencyAwareWindowSize) {
        this.latencyAwareWindowSize = latencyAwareWindowSize;
        return this;
    }

    @Override
    public float getLatencyAwareSentinelCompare() {
        return latencyAwareSentinelCompare;
    }

    public ConnectionPoolConfigurationImpl setLatencyAwareSentinelCompare(float latencyAwareSentinelCompare) {
        this.latencyAwareSentinelCompare = latencyAwareSentinelCompare;
        return this;
    }

    @Override
    public float getLatencyAwareBadnessThreshold() {
        return this.latencyAwareBadnessThreshold;
    }

    public ConnectionPoolConfigurationImpl setLatencyAwareBadnessThreshold(float threshold) {
        this.latencyAwareBadnessThreshold = threshold;
        return this;
    }

    @Override
    public int getConnectionLimiterWindowSize() {
        return this.connectionLimiterWindowSize;
    }

    public ConnectionPoolConfigurationImpl setConnectionLimiterWindowSize(int pendingConnectionWindowSize) {
        this.connectionLimiterWindowSize = pendingConnectionWindowSize;
        return this;
    }

    @Override
    public int getConnectionLimiterMaxPendingCount() {
        return this.connectionLimiterMaxPendingCount;
    }

    public ConnectionPoolConfigurationImpl setConnectionLimiterMaxPendingCount(int connectionLimiterMaxPendingCount) {
        this.connectionLimiterMaxPendingCount = connectionLimiterMaxPendingCount;
        return this;
    }

    @Override
    public int getMaxPendingConnectionsPerHost() {
        return this.maxPendingConnectionsPerHost;
    }

    public ConnectionPoolConfigurationImpl setMaxPendingConnectionsPerHost(int maxPendingConnectionsPerHost) {
        this.maxPendingConnectionsPerHost = maxPendingConnectionsPerHost;
        return this;
    }

    @Override
    public int getMaxBlockedThreadsPerHost() {
        return this.maxBlockedThreadsPerHost;
    }

    public ConnectionPoolConfigurationImpl setMaxBlockedThreadsPerHost(int maxBlockedThreadsPerHost) {
        this.maxBlockedThreadsPerHost = maxBlockedThreadsPerHost;
        return this;
    }

    @Override
    public int getTimeoutWindow() {
        return this.timeoutWindow;
    }

    public ConnectionPoolConfigurationImpl setTimeoutWindow(int timeoutWindow) {
        this.timeoutWindow = timeoutWindow;
        return this;
    }

    @Override
    public int getMaxTimeoutCount() {
        return this.maxTimeoutCount;
    }

    public ConnectionPoolConfigurationImpl setMaxTimeoutCount(int maxTimeoutCount) {
        this.maxTimeoutCount = maxTimeoutCount;
        return this;
    }

    @Override
    public int getLatencyAwareUpdateInterval() {
        return latencyAwareUpdateInterval;
    }

    public ConnectionPoolConfigurationImpl setLatencyAwareUpdateInterval(int latencyAwareUpdateInterval) {
        this.latencyAwareUpdateInterval = latencyAwareUpdateInterval;
        return this;
    }

    @Override
    public int getLatencyAwareResetInterval() {
        return latencyAwareResetInterval;
    }

    public ConnectionPoolConfigurationImpl setLatencyAwareResetInterval(int latencyAwareResetInterval) {
        this.latencyAwareResetInterval = latencyAwareResetInterval;
        return this;
    }

    @Override
    public int getRetrySuspendWindow() {
        return this.retrySuspendWindow;
    }

    public ConnectionPoolConfigurationImpl setRetrySuspendWindow(int retrySuspendWindow) {
        this.retrySuspendWindow = retrySuspendWindow;
        return this;
    }

    @Override
    public int getMaxOperationsPerConnection() {
        return maxOperationsPerConnection;
    }

    public ConnectionPoolConfigurationImpl setMaxOperationsPerConnection(int maxOperationsPerConnection) {
        this.maxOperationsPerConnection = maxOperationsPerConnection;
        return this;
    }

    @Override
    public LatencyScoreStrategy getLatencyScoreStrategy() {
        return this.latencyScoreStrategy;
    }

    public ConnectionPoolConfigurationImpl setLatencyScoreStrategy(LatencyScoreStrategy latencyScoreStrategy) {
        this.latencyScoreStrategy = latencyScoreStrategy;
        return this;
    }

    @Override
    public BadHostDetector getBadHostDetector() {
        return badHostDetector;
    }

    public ConnectionPoolConfigurationImpl setBadHostDetector(BadHostDetector badHostDetector) {
        this.badHostDetector = badHostDetector;
        return this;
    }

    @Override
    public int getRetryMaxDelaySlice() {
        return retryMaxDelaySlice;
    }

    public ConnectionPoolConfigurationImpl setRetryMaxDelaySlice(int retryMaxDelaySlice) {
        this.retryMaxDelaySlice = retryMaxDelaySlice;
        return this;
    }

    @Override
    public int getRetryDelaySlice() {
        return this.retryDelaySlice;
    }

    public ConnectionPoolConfigurationImpl setRetryDelaySlice(int retryDelaySlice) {
        this.retryDelaySlice = retryDelaySlice;
        return this;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

    @Override
    public AuthenticationCredentials getAuthenticationCredentials() {
        return credentials;
    }

    public ConnectionPoolConfigurationImpl setAuthenticationCredentials(AuthenticationCredentials credentials) {
        this.credentials = credentials;
        return this;
    }

    @Override
    public OperationFilterFactory getOperationFilterFactory() {
        return filterFactory;
    }
    
    public ConnectionPoolConfigurationImpl setOperationFilterFactory(OperationFilterFactory filterFactory) {
        this.filterFactory = filterFactory;
        return this;
    }

    @Override
    public Partitioner getPartitioner() {
        return this.partitioner;
    }

    public ConnectionPoolConfigurationImpl setPartitioner(Partitioner partitioner) {
        this.partitioner = partitioner;
        return this;
    }

    @Override
    public int getBlockedThreadThreshold() {
        return this.blockedThreadThreshold;
    }

    public ConnectionPoolConfigurationImpl setBlockedThreadThreshold(int threshold) {
        this.blockedThreadThreshold = threshold;
        return this;
    }
    
    @Override
    public float getMinHostInPoolRatio() {
        return this.minHostInPoolRatio;
    }
    
    public ConnectionPoolConfigurationImpl setMinHostInPoolRatio(float ratio) {
        this.minHostInPoolRatio = ratio;
        return this;
    }

    public SSLConnectionContext getSSLConnectionContext() {
        return sslCtx;
    }

    public ConnectionPoolConfigurationImpl setSSLConnectionContext(SSLConnectionContext sslCtx) {
        this.sslCtx = sslCtx;
        return this;
    }

    @Override
    public ScheduledExecutorService getMaintainanceScheduler() {
        return maintainanceExecutor;
    }

    public ConnectionPoolConfigurationImpl setMaintainanceScheduler(ScheduledExecutorService executor) {
        maintainanceExecutor = executor;
        bOwnMaintainanceExecutor = false;
        return this;
    }

    @Override
    public ScheduledExecutorService getHostReconnectExecutor() {
        return this.reconnectExecutor;
    }

    public ConnectionPoolConfigurationImpl setHostReconnectExecutor(ScheduledExecutorService executor) {
        reconnectExecutor = executor;
        bOwnReconnectExecutor = false;
        return this;
    }

}
