package com.netflix.astyanax.connectionpool.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.HostConnectionPool;

public class Slf4jConnectionPoolMonitorImpl extends CountingConnectionPoolMonitor {
	private static final Logger LOG = LoggerFactory.getLogger(Slf4jConnectionPoolMonitorImpl.class);

	@Override
	public void incOperationFailure(Host host, Exception reason) {
		super.incOperationFailure(host, reason);
		
		LOG.info(reason.getMessage());
	}

	@Override
	public void incConnectionClosed(Host host, Exception reason) {
		super.incConnectionClosed(host, reason);
		
		if (reason != null) {
			LOG.info(reason.getMessage());
		}
	}

	@Override
	public void incConnectionCreateFailed(Host host, Exception reason) {
		super.incConnectionCreateFailed(host, reason);
		if (reason != null) {
			LOG.info(reason.getMessage());
		}
	}

	@Override
	public void incFailover(Host host, Exception reason) {
		super.incFailover(host, reason);
		if (reason != null) {
			LOG.info(reason.getMessage());
		}
	}

	@Override
	public void onHostAdded(Host host, HostConnectionPool<?> pool) {
		super.onHostAdded(host, pool);
		LOG.info("HostAdded: " + host);
	}

	@Override
	public void onHostRemoved(Host host) {
		super.onHostRemoved(host);
		LOG.info("HostRemoved: " + host);
	}

	@Override
	public void onHostDown(Host host, Exception reason) {
		super.onHostDown(host, reason);
		LOG.info("HostDown: " + host);
	}

	@Override
	public void onHostReactivated(Host host, HostConnectionPool<?> pool) {
		super.onHostReactivated(host, pool);
		LOG.info("HostReactivated: " + host);
	}
}
