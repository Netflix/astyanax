package com.netflix.astyanax.cql;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import com.datastax.driver.core.Configuration;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.AuthenticationCredentials;
import com.netflix.astyanax.connectionpool.BadHostDetector;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.LatencyScoreStrategy;
import com.netflix.astyanax.connectionpool.OperationFilterFactory;
import com.netflix.astyanax.connectionpool.RetryBackoffStrategy;
import com.netflix.astyanax.connectionpool.SSLConnectionContext;
import com.netflix.astyanax.connectionpool.impl.HostSelectorStrategy;
import com.netflix.astyanax.partitioner.Partitioner;

/**
 * This class simply acts as a holder for the {@link Configuration} object for the java driver. It can be injected into the 
 * {@link AstyanaxContext} via the regular interface and thus helps serve as a bridge when setting up the java driver using the 
 * regular Astyanax setup route. 
 * 
 * The class does not actually implement any of the actual methods of {@link ConnectionPoolConfiguration}. It's sole purpose is just to 
 * hold a reference to the java driver config object and then be injected via the regular interfaces available in AstyanaxContext. 
 * 
 * @author poberai
 *
 */
public class JavaDriverConnectionPoolConfigurationImpl implements ConnectionPoolConfiguration {

	private final Configuration jdConfig;
	
	public JavaDriverConnectionPoolConfigurationImpl(Configuration configuration) {
		this.jdConfig = configuration;
	}

	public Configuration getJavaDriverConfig() {
		return jdConfig;
	}

	@Override
	public LatencyScoreStrategy getLatencyScoreStrategy() {
		return null;
	}

	@Override
	public BadHostDetector getBadHostDetector() {
		return null;
	}

	@Override
	public int getPort() {
		return jdConfig.getProtocolOptions().getPort();
	}

	@Override
	public String getName() {
		return null;
	}

	@Override
	public int getMaxConnsPerHost() {
		return 0;
	}

	@Override
	public int getInitConnsPerHost() {
		return 0;
	}

	@Override
	public int getMaxConns() {
		return 0;
	}

	@Override
	public int getMaxTimeoutWhenExhausted() {
		return 0;
	}

	@Override
	public int getMaxFailoverCount() {
		return 0;
	}

	@Override
	public RetryBackoffStrategy getRetryBackoffStrategy() {
		return null;
	}

	@Override
	public HostSelectorStrategy getHostSelectorStrategy() {
		return null;
	}

	@Override
	public String getSeeds() {
		return null;
	}

	@Override
	public List<Host> getSeedHosts() {
		return null;
	}

	@Override
	public String getLocalDatacenter() {
		return null;
	}

	@Override
	public int getSocketTimeout() {
		return 0;
	}

	@Override
	public int getConnectTimeout() {
		return 0;
	}

	@Override
	public int getConnectionLimiterWindowSize() {
		return 0;
	}

	@Override
	public int getConnectionLimiterMaxPendingCount() {
		return 0;
	}

	@Override
	public int getLatencyAwareWindowSize() {
		return 0;
	}

	@Override
	public float getLatencyAwareSentinelCompare() {
		return 0;
	}

	@Override
	public float getLatencyAwareBadnessThreshold() {
		return 0;
	}

	@Override
	public int getBlockedThreadThreshold() {
		return 0;
	}

	@Override
	public float getMinHostInPoolRatio() {
		return 0;
	}

	@Override
	public int getLatencyAwareUpdateInterval() {
		return 0;
	}

	@Override
	public int getLatencyAwareResetInterval() {
		return 0;
	}

	@Override
	public int getMaxPendingConnectionsPerHost() {
		return 0;
	}

	@Override
	public int getMaxBlockedThreadsPerHost() {
		return 0;
	}

	@Override
	public int getTimeoutWindow() {
		return 0;
	}

	@Override
	public int getMaxTimeoutCount() {
		return 0;
	}

	@Override
	public int getRetrySuspendWindow() {
		return 0;
	}

	@Override
	public int getRetryMaxDelaySlice() {
		return 0;
	}

	@Override
	public int getRetryDelaySlice() {
		return 0;
	}

	@Override
	public int getMaxOperationsPerConnection() {
		return 0;
	}

	@Override
	public AuthenticationCredentials getAuthenticationCredentials() {
		return null;
	}

	@Override
	public OperationFilterFactory getOperationFilterFactory() {
		return null;
	}

	@Override
	public Partitioner getPartitioner() {
		return null;
	}

	@Override
	public SSLConnectionContext getSSLConnectionContext() {
		return null;
	}

	@Override
	public ScheduledExecutorService getMaintainanceScheduler() {
		return null;
	}

	@Override
	public ScheduledExecutorService getHostReconnectExecutor() {
		return null;
	}

	@Override
	public void initialize() {
	}

	@Override
	public void shutdown() {
	}
}
