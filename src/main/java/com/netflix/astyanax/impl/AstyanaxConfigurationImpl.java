package com.netflix.astyanax.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.astyanax.AstyanaxConfiguration;
import com.netflix.astyanax.Clock;
import com.netflix.astyanax.clock.MicrosecondsSyncClock;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.retry.RetryPolicy;
import com.netflix.astyanax.retry.RunOnce;
import com.netflix.astyanax.util.StringUtils;

public class AstyanaxConfigurationImpl implements AstyanaxConfiguration {
	private ConsistencyLevel defaultReadConsistencyLevel = ConsistencyLevel.CL_ONE;
	private ConsistencyLevel defaultWriteConsistencyLevel = ConsistencyLevel.CL_ONE;
	private Clock clock = new MicrosecondsSyncClock();
	private RetryPolicy retryPolicy = RunOnce.get();
	private ExecutorService asyncExecutor = Executors.newFixedThreadPool(5, new ThreadFactoryBuilder().setDaemon(true).build());
	private NodeDiscoveryType discoveryType = NodeDiscoveryType.NONE;
	private int discoveryIntervalInSeconds = 30;
	private ConnectionPoolType connectionPoolType = ConnectionPoolType.ROUND_ROBIN;
	
    public AstyanaxConfigurationImpl() {
	}

	public AstyanaxConfigurationImpl setConnectionPoolType(ConnectionPoolType connectionPoolType) {
		this.connectionPoolType = connectionPoolType;
		return this;
	}
	
	@Override
	public ConnectionPoolType getConnectionPoolType() {
		return this.connectionPoolType;
	}
	
	@Override
	public ConsistencyLevel getDefaultReadConsistencyLevel() {
		return this.defaultReadConsistencyLevel;
	}
	
	public AstyanaxConfigurationImpl setDefaultReadConsistencyLevel(ConsistencyLevel cl) {
		this.defaultReadConsistencyLevel = cl;
		return this;
	}
	
	@Override
	public ConsistencyLevel getDefaultWriteConsistencyLevel() {
		return this.defaultWriteConsistencyLevel;
	}
	
	public AstyanaxConfigurationImpl setDefaultWriteConsistencyLevel(ConsistencyLevel cl) {
		this.defaultWriteConsistencyLevel = cl;
		return this;
	}

	@Override
	public Clock getClock() {
		return this.clock;
	}
	
	public AstyanaxConfigurationImpl setClock(Clock clock) {
		this.clock = clock;
		return this;
	}

	@Override
	public ExecutorService getAsyncExecutor() {
		return asyncExecutor;
	}
	
	public AstyanaxConfigurationImpl setAsyncExecutor(ExecutorService executor) {
		this.asyncExecutor.shutdown();
		this.asyncExecutor = executor;
		return this;
	}
	
	@Override
	public RetryPolicy getRetryPolicy() {
		return retryPolicy;
	}
	
	public AstyanaxConfigurationImpl setRetryPolicy(RetryPolicy retryPolicy) {
		this.retryPolicy = retryPolicy;
		return this;
	}

	public String toString() {
		return StringUtils.joinClassGettersValues(this, "A6xConfig", AstyanaxConfigurationImpl.class);
	}

	@Override
	public int getDiscoveryDelayInSeconds() {
		return discoveryIntervalInSeconds;
	}

	public AstyanaxConfigurationImpl setDiscoveryDelayInSeconds(int delay) {
		this.discoveryIntervalInSeconds = delay;
		return this;
	}

	@Override
	public NodeDiscoveryType getDiscoveryType() {
		return discoveryType;
	}
	
	public AstyanaxConfigurationImpl setDiscoveryType(NodeDiscoveryType discoveryType) {
		this.discoveryType = discoveryType;
		return this;
	}
}
