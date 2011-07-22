package com.netflix.astyanax.shallows;

import org.apache.log4j.Logger;

import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.HostConnectionPool;

public class EmptyConnectionPoolMonitor implements ConnectionPoolMonitor {
	private static Logger LOG = Logger.getLogger(EmptyConnectionPoolMonitor.class);
	
	public static EmptyConnectionPoolMonitor instance = new EmptyConnectionPoolMonitor();
	
	@Override
	public void incOperationSuccess(Host host, long latency) {
	}

	@Override
	public void incPoolExhaustedTimeout() {
	}

	@Override
	public void incOperationTimeout() {
	}

	@Override
	public void incConnectionBorrowed(Host host, long delay) {
	}

	@Override
	public void incConnectionReturned(Host host) {
	}

	@Override
	public void onHostAdded(Host host, HostConnectionPool<?> pool) {
		LOG.info(String.format("Added host " + host));
	}

	@Override
	public void onHostRemoved(Host host) {
		LOG.info(String.format("Remove host " + host));
	}

	@Override
	public void onHostDown(Host host, Exception reason) {
		LOG.warn(String.format("Downed host " + host + " reason=\"" + reason + "\""));
	}

	@Override
	public void onHostReactivated(Host host, HostConnectionPool<?> pool) {
		LOG.info(String.format("Reactivating host " + host));
	}

	@Override
	public void incFailover() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void incConnectionCreated(Host host) {
	}

	@Override
	public void incConnectionCreateFailed(Host host, Exception e) {
	}

	@Override
	public void incNoHosts() {
	}

	@Override
	public void incOperationFailure(Host host, Exception e) {
	}
}
