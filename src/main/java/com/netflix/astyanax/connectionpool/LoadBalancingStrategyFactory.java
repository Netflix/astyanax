package com.netflix.astyanax.connectionpool;

public interface LoadBalancingStrategyFactory {
	LoadBalancingStrategy createLoadBalancingStrategy(ConnectionPoolConfiguration config);
}
