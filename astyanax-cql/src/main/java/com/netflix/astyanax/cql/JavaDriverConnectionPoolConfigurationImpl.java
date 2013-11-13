package com.netflix.astyanax.cql;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import com.datastax.driver.core.Configuration;
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

public class JavaDriverConnectionPoolConfigurationImpl implements ConnectionPoolConfiguration {

	private Configuration jdConfig = new Configuration(); 
	
	public JavaDriverConnectionPoolConfigurationImpl withJavaDriverConfig(Configuration jdCfg) {
		jdConfig = jdCfg;
		return this;
	}
	
	public Configuration getJavaDriverConfig() {
		return jdConfig;
	}

	@Override
	public LatencyScoreStrategy getLatencyScoreStrategy() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BadHostDetector getBadHostDetector() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getPort() {
		return jdConfig.getProtocolOptions().getPort();
	}

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getMaxConnsPerHost() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getInitConnsPerHost() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxConns() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxTimeoutWhenExhausted() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxFailoverCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public RetryBackoffStrategy getRetryBackoffStrategy() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public HostSelectorStrategy getHostSelectorStrategy() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getSeeds() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Host> getSeedHosts() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getLocalDatacenter() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getSocketTimeout() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getConnectTimeout() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getConnectionLimiterWindowSize() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getConnectionLimiterMaxPendingCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getLatencyAwareWindowSize() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public float getLatencyAwareSentinelCompare() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public float getLatencyAwareBadnessThreshold() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getBlockedThreadThreshold() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public float getMinHostInPoolRatio() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getLatencyAwareUpdateInterval() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getLatencyAwareResetInterval() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxPendingConnectionsPerHost() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxBlockedThreadsPerHost() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getTimeoutWindow() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxTimeoutCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getRetrySuspendWindow() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getRetryMaxDelaySlice() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getRetryDelaySlice() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxOperationsPerConnection() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public AuthenticationCredentials getAuthenticationCredentials() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public OperationFilterFactory getOperationFilterFactory() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Partitioner getPartitioner() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SSLConnectionContext getSSLConnectionContext() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ScheduledExecutorService getMaintainanceScheduler() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ScheduledExecutorService getHostReconnectExecutor() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void initialize() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void shutdown() {
		// TODO Auto-generated method stub
		
	}

}
