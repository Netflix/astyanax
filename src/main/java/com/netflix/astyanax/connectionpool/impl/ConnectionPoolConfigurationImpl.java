package com.netflix.astyanax.connectionpool.impl;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import com.google.inject.internal.Preconditions;
import com.netflix.astyanax.Clock;
import com.netflix.astyanax.KeyspaceTracers;
import com.netflix.astyanax.connectionpool.BadHostDetector;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.ConnectionPoolFactory;
import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.ExhaustedStrategy;
import com.netflix.astyanax.connectionpool.FailoverStrategy;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.LoadBalancingStrategy;
import com.netflix.astyanax.connectionpool.NodeDiscoveryFactory;
import com.netflix.astyanax.connectionpool.RetryBackoffStrategy;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.shallows.EmptyConnectionPoolMonitor;
import com.netflix.astyanax.shallows.EmptyKeyspaceTracers;
import com.netflix.astyanax.shallows.EmptyNodeDiscoveryFactoryImpl;

public class ConnectionPoolConfigurationImpl implements ConnectionPoolConfiguration {
    /**
     * Default values
     */
    public static final Integer DEFAULT_MAX_TIME_WHEN_EXHAUSTED = 2000;
    public static final Integer DEFAULT_AUTO_DISCOVERY_DELAY_IN_SECONDS = 30;
    public static final Integer DEFAULT_SOCKET_TIMEOUT = 2000;	// ms
    public static final Integer DEFAULT_MAX_ACTIVE_PER_PARTITION = 3;
    public static final Integer DEFAULT_PORT = 7102;
    public static final Integer DEFAULT_FAILOVER_COUNT = 0;
    public static final Integer DEFAULT_FAILOVER_WAIT_TIME = 0;
    public static final Integer DEFAULT_MAX_EXHAUSTED_RETRIES = 0;
    public static final ConsistencyLevel DEFAULT_READ_CONSISTENCY = ConsistencyLevel.CL_ONE;
    public static final ConsistencyLevel DEFAULT_WRITE_CONSISTENCY = ConsistencyLevel.CL_ONE;
    public static final Boolean DEFAULT_ENABLE_RING_DESCRIBE = true;
    public static final Boolean DEFAULT_DEBUG_ENABLED = false;
    public static final String DEFAULT_RING_IP_FILTER = null;
    public static final ConnectionPoolMonitor DEFAULT_MONITOR = new EmptyConnectionPoolMonitor();
    public static final KeyspaceTracers DEFAULT_KEYSPACE_TRACERS = new EmptyKeyspaceTracers();
    public static final Factory<LoadBalancingStrategy> DEFAULT_LOAD_BALACING_STRATEGY = new Factory<LoadBalancingStrategy>() {
		@Override
		public LoadBalancingStrategy createInstance(
				ConnectionPoolConfiguration config) {
			return new RoundRobinLoadBalancingStrategy(config);
		}
    };
    public static final Factory<FailoverStrategy> DEFAULT_FAILOVER_STRATEGY = new Factory<FailoverStrategy>() {
		@Override
		public FailoverStrategy createInstance(
				ConnectionPoolConfiguration config) {
			return new FailoverStrategyImpl(config.getMaxFailoverCount(), config.getFailoverWaitTime());
		}
    };
    public static final Factory<ExhaustedStrategy> DEFAULT_EXHAUSTED_STRATEGY = new Factory<ExhaustedStrategy>() {
		@Override
		public ExhaustedStrategy createInstance(
				ConnectionPoolConfiguration config) {
			return new ExhaustedStrategyImpl(config.getMaxExhaustedRetries(), config.getMaxTimeoutWhenExhausted());
		}
    };
    
	private final String keyspaceName;
	private final String clusterName;
    
    private boolean isRingDescribeEnabled = DEFAULT_ENABLE_RING_DESCRIBE;
    private boolean isDebugEnabled = DEFAULT_DEBUG_ENABLED;
    private ConnectionPoolFactory connectionPoolFactory = null;
	private int autoDiscoveryDelay = DEFAULT_AUTO_DISCOVERY_DELAY_IN_SECONDS;
	private ConsistencyLevel defaultReadConsistencyLevel = ConsistencyLevel.CL_ONE;
	private ConsistencyLevel defaultWriteConsistencyLevel = ConsistencyLevel.CL_ONE;
	private int maxConnsPerPartition = DEFAULT_MAX_ACTIVE_PER_PARTITION;
	private int port = DEFAULT_PORT;
	private int socketTimeout = DEFAULT_SOCKET_TIMEOUT;
	private int maxFailoverCount = DEFAULT_FAILOVER_COUNT;
	private int failoverWaitTime = DEFAULT_FAILOVER_WAIT_TIME;
	private int maxExhaustedRetries = DEFAULT_MAX_EXHAUSTED_RETRIES;
	private String seeds = null;
	private int maxTimeoutWhenExhausted = DEFAULT_MAX_TIME_WHEN_EXHAUSTED;
	private RetryBackoffStrategy hostRetryBackoffStrategy = new ExponentialRetryBackoffStrategy(10, 1000, 10000);
	private String ringIpFilter = DEFAULT_RING_IP_FILTER;
	private Clock clock = new MillisecondsClock();
	private NodeDiscoveryFactory nodeDiscoveryFactory = EmptyNodeDiscoveryFactoryImpl.get();
	private Factory<LoadBalancingStrategy> loadBalancingStrategyFactory = DEFAULT_LOAD_BALACING_STRATEGY;
	private Factory<ExhaustedStrategy> exhaustedStrategyFactory = DEFAULT_EXHAUSTED_STRATEGY;
	private Factory<FailoverStrategy> failoverStrategyFactory = DEFAULT_FAILOVER_STRATEGY;
	private ConnectionPoolMonitor monitor = DEFAULT_MONITOR;
	private KeyspaceTracers keyspaceTracers = DEFAULT_KEYSPACE_TRACERS;
	private BadHostDetector badHostDetector = new BadHostDetectorImpl(3, 1000);
	
	public ConnectionPoolConfigurationImpl(String clusterName, String keyspaceName) {
		this.keyspaceName = keyspaceName;
		this.clusterName = clusterName;
	}
	
	/* (non-Javadoc)
	 * @see com.netflix.cassandra.ConnectionPoolConfiguration#isRingDescribeEnabled()
	 */
	@Override
	public boolean isRingDescribeEnabled() {
		return isRingDescribeEnabled;
	}
	
	public void setIsRingDescribeEnabled(boolean enabled) {
		this.isRingDescribeEnabled = enabled;
	}
	
	/* (non-Javadoc)
	 * @see com.netflix.cassandra.ConnectionPoolConfiguration#getKeyspaceName()
	 */
	@Override
	public String getKeyspaceName() {
		return this.keyspaceName;
	}
	
	/* (non-Javadoc)
	 * @see com.netflix.cassandra.ConnectionPoolConfiguration#getSocketTimeout()
	 */
	@Override
	public int getSocketTimeout() {
		return socketTimeout;
	}
	
	public void setSocketTimeout(int socketTimeout) { 
		this.socketTimeout = socketTimeout;
	}
	
	/* (non-Javadoc)
	 * @see com.netflix.cassandra.ConnectionPoolConfiguration#getSeeds()
	 */
	@Override
	public String getSeeds() {
		return this.seeds;
	}
	
	public void setSeeds(String seeds) {
		this.seeds = seeds;
	}
	
	/* (non-Javadoc)
	 * @see com.netflix.cassandra.ConnectionPoolConfiguration#getMaxTimeoutWhenExhausted()
	 */
	@Override
	public int getMaxTimeoutWhenExhausted() {
		return this.maxTimeoutWhenExhausted;
	}
	
	public void setMaxTimeoutWhenExhausted(int timeout) {
		this.maxTimeoutWhenExhausted = timeout;
	}
	
	/* (non-Javadoc)
	 * @see com.netflix.cassandra.ConnectionPoolConfiguration#getPort()
	 */
	@Override
	public int getPort() {
		return this.port;
	}
	
	public void setPort(int port) {
		this.port = port;
	}
	
	/* (non-Javadoc)
	 * @see com.netflix.cassandra.ConnectionPoolConfiguration#getMaxConnsPerHost()
	 */
	@Override
	public int getMaxConnsPerHost() {
		return this.maxConnsPerPartition;
	}
	
	public void setMaxConnsPerHost(int maxConns) {
		this.maxConnsPerPartition = maxConns;
	}
	
	/* (non-Javadoc)
	 * @see com.netflix.cassandra.ConnectionPoolConfiguration#getDefaultReadConsistencyLevel()
	 */
	@Override
	public ConsistencyLevel getDefaultReadConsistencyLevel() {
		return this.defaultReadConsistencyLevel;
	}
	
	public void setDefaultReadConsistencyLevel(ConsistencyLevel cl) {
		this.defaultReadConsistencyLevel = cl;
	}
	
	/* (non-Javadoc)
	 * @see com.netflix.cassandra.ConnectionPoolConfiguration#getDefaultWriteConsistencyLevel()
	 */
	@Override
	public ConsistencyLevel getDefaultWriteConsistencyLevel() {
		return this.defaultWriteConsistencyLevel;
	}
	
	public void setDefaultWriteConsistencyLevel(ConsistencyLevel cl) {
		this.defaultWriteConsistencyLevel = cl;
	}

	/* (non-Javadoc)
	 * @see com.netflix.cassandra.ConnectionPoolConfiguration#getRetryBackoffStrategy()
	 */
	@Override
	public RetryBackoffStrategy getRetryBackoffStrategy() {
		return this.hostRetryBackoffStrategy;
	}
	
	public void setRetryBackoffStrategy(RetryBackoffStrategy hostRetryBackoffStrategy) {
		this.hostRetryBackoffStrategy = hostRetryBackoffStrategy;
	}
	
	/* (non-Javadoc)
	 * @see com.netflix.cassandra.ConnectionPoolConfiguration#getAutoDiscoveryDelay()
	 */
	@Override
	public int getAutoDiscoveryDelay() {
		return this.autoDiscoveryDelay;
	}
	
	public void setAutoDiscoveryDelay(int seconds) {
		this.autoDiscoveryDelay = seconds;
	}
	
	/* (non-Javadoc)
	 * @see com.netflix.cassandra.ConnectionPoolConfiguration#getRingIpFilter()
	 */
	@Override
	public String getRingIpFilter() {
		return this.ringIpFilter;
	}
	
	public void setRingIpFilter(String filter) {
		this.ringIpFilter = filter;
	}
	
	/* (non-Javadoc)
	 * @see com.netflix.cassandra.ConnectionPoolConfiguration#getSeedHosts()
	 */
	@Override
	public List<Host> getSeedHosts() {
		List<Host> hosts = new ArrayList<Host>();
		for (String seed : seeds.split(",")) {
			seed = seed.trim();
			if (seed.length() > 0) {
				hosts.add(new Host(seed, this.port));
			}
		}
		return hosts;
	}
	
	@Override
	public ConnectionPoolFactory getConnectionPoolFactory() {
		return this.connectionPoolFactory;
	}
	
	public void setConnectionPoolFactory(ConnectionPoolFactory factory) {
		this.connectionPoolFactory = factory;
	}

	@Override
	public NodeDiscoveryFactory getNodeDiscoveryFactory() {
		return this.nodeDiscoveryFactory;
	}
	
	public void setNodeDiscoveryFactory(NodeDiscoveryFactory factory) {
		Preconditions.checkNotNull(factory, "NodeDiscoveryFactory cannot be null");
		this.nodeDiscoveryFactory = factory;
	}

	@Override
	public Clock getClock() {
		return this.clock;
	}
	
	public void setClock(Clock clock) {
		this.clock = clock;
	}

	@Override
	public Factory<LoadBalancingStrategy> getLoadBlancingPolicyFactory() {
		return this.loadBalancingStrategyFactory;
	}
	
	public void setLoadBlancingStrategyFactory(Factory<LoadBalancingStrategy> factory) {
		this.loadBalancingStrategyFactory = factory;
	}
	
	public String toString() {
		Field[] fields = getClass().getSuperclass().getDeclaredFields();
		StringBuilder sb = new StringBuilder();
		for (Field field : fields) {
			if (Character.isLowerCase(field.getName().charAt(0))) {
				if (field.getType() == int.class) {
					
				}
				if (sb.length() == 0) {
					sb.append("ConnectionPoolConfigurationImpl[");
				}
				else {
					sb.append(',');
				}
				sb.append(field.getName()).append('=');
				try {
					Object value = field.get(this);
					sb.append(value);
				} catch (IllegalArgumentException e) {
					sb.append("***");
				} catch (IllegalAccessException e) {
					sb.append("***");
				}
			}
		}
		sb.append("]");
		return sb.toString();
	}

	@Override
	public ConnectionPoolMonitor getConnectionPoolMonitor() {
		return this.monitor;
	}
	
	public void setConnectionPoolMonitor(ConnectionPoolMonitor monitor) {
		this.monitor = monitor;
	}

	@Override
	public KeyspaceTracers getKeyspaceTracers() {
		return keyspaceTracers;
	}
	
	public void setKeyspaceTracers(KeyspaceTracers keyspaceTracers) {
		this.keyspaceTracers = keyspaceTracers;
	}

	@Override
	public BadHostDetector getBadHostDetector() {
		return badHostDetector;
	}
	
	public void setBadHostDetector(BadHostDetector detector) {
		this.badHostDetector = detector;
	}

	@Override
	public Factory<ExhaustedStrategy> getExhaustedStrategyFactory() {
		return this.exhaustedStrategyFactory;
	}

	public void setExhaustedStrategyFactory(Factory<ExhaustedStrategy> exhaustedStrategyFactory) {
		this.exhaustedStrategyFactory = exhaustedStrategyFactory;
	}

	@Override
	public Factory<FailoverStrategy> getFailoverStrategyFactory() {
		return this.failoverStrategyFactory;
	}

	public void setFailoverStrategyFactory(Factory<FailoverStrategy> failoverStrategyFactory) {
		this.failoverStrategyFactory = failoverStrategyFactory;
	}

	@Override
	public String getClusterName() {
		return this.clusterName;
	}

	@Override
	public int getMaxFailoverCount() {
		return this.maxFailoverCount;
	}

	public void setMaxFailoverCount(int maxFailoverCount) {
		this.maxFailoverCount = maxFailoverCount;
	}

	@Override
	public int getFailoverWaitTime() {
		return this.failoverWaitTime;
	}
	
	public  void setFailoverWaitTime(int failoverWaitTime) {
		this.failoverWaitTime = failoverWaitTime;
	}

	@Override
	public int getMaxExhaustedRetries() {
		return maxExhaustedRetries;
	}
	
	public void setMaxExhaustedRetries(int maxExhaustedRetries) {
		this.maxExhaustedRetries = maxExhaustedRetries;
	}

	@Override
	public boolean isDebugEnabled() {
		return this.isDebugEnabled;
	}
	
	public void setIsDebugEnabled(boolean isDebugEnabled) {
		this.isDebugEnabled = isDebugEnabled;
	}
}
