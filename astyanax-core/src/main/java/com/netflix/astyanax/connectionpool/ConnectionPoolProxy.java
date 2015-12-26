package com.netflix.astyanax.connectionpool;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.connectionpool.impl.Topology;
import com.netflix.astyanax.partitioner.Partitioner;
import com.netflix.astyanax.retry.RetryPolicy;

public class ConnectionPoolProxy<T> implements ConnectionPool<T> {

	private static final Logger Logger = LoggerFactory.getLogger(ConnectionPoolProxy.class);
	
	private AtomicReference<SeedHostListener> listener = new AtomicReference<SeedHostListener>(null);
	private AtomicReference<Collection<Host>> lastHostList = new AtomicReference<Collection<Host>>(null);
	
	private final ConnectionPoolConfiguration cpConfig;
	private final ConnectionPoolMonitor monitor;
	
	public ConnectionPoolProxy(ConnectionPoolConfiguration cpConfig, ConnectionFactory<T> connectionFactory, ConnectionPoolMonitor monitor) {
		this.cpConfig = cpConfig;
		this.monitor = monitor;
	}

	public ConnectionPoolConfiguration getConnectionPoolConfiguration() {
		return cpConfig;
	}

	public ConnectionPoolMonitor getConnectionPoolMonitor() {
		return monitor;
	}

	@Override
	public void setHosts(Collection<Host> hosts) {
		
		if (hosts != null) {
			Logger.info("Setting hosts for listener here: " + listener.getClass().getName() +  "   " + hosts);
			lastHostList.set(hosts);
		}
		
		if (listener.get() != null) {
			Logger.info("Setting hosts for listener: " + listener.getClass().getName() +  "   " + hosts);
			listener.get().setHosts(hosts, cpConfig.getPort());
		}
	}
	
	public Collection<Host> getHosts() {
		return lastHostList.get();
	}

	public interface SeedHostListener {
		public void setHosts(Collection<Host> hosts, int port);
		public void shutdown();
	}
	
	public void addListener(SeedHostListener listener) {
		this.listener.set(listener);
		if (this.lastHostList.get() != null) {
			this.listener.get().setHosts(lastHostList.get(), cpConfig.getPort());
		}
	}

	@Override
	public boolean addHost(Host host, boolean refresh) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean removeHost(Host host, boolean refresh) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isHostUp(Host host) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean hasHost(Host host) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public List<HostConnectionPool<T>> getActivePools() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<HostConnectionPool<T>> getPools() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public HostConnectionPool<T> getHostPool(Host host) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <R> OperationResult<R> executeWithFailover(Operation<T, R> op, RetryPolicy retry) throws ConnectionException, OperationException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void shutdown() {
		if (this.lastHostList.get() != null) {
			this.listener.get().shutdown();
		}
	}

	@Override
	public void start() {
		// TODO Auto-generated method stub
	}

	@Override
	public Topology<T> getTopology() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Partitioner getPartitioner() {
		// TODO Auto-generated method stub
		return null;
	}

}
