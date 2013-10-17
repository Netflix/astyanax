package com.netflix.astyanax.connectionpool;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.connectionpool.impl.Topology;
import com.netflix.astyanax.partitioner.Partitioner;
import com.netflix.astyanax.retry.RetryPolicy;

public class CqlConnectionPoolProxy<T> implements ConnectionPool<T> {

	private static final Logger Logger = LoggerFactory.getLogger(CqlConnectionPoolProxy.class);
	
	private AtomicReference<SeedHostListener> listener = new AtomicReference<SeedHostListener>(null);
	private AtomicBoolean firstRound = new AtomicBoolean(true);
	
	public CqlConnectionPoolProxy(ConnectionPoolConfiguration cpConfig,
			ConnectionFactory<T> connectionFactory,
			ConnectionPoolMonitor monitor) {
	}


	@Override
	public void setHosts(Collection<Host> hosts) {
		
		if (firstRound.get() && listener.get() != null) {
			Logger.info("Setting hosts for listener: " + listener.getClass().getName() +  "   " + hosts);
			listener.get().setHosts(hosts);
		}
	}

	public interface SeedHostListener {
		public void setHosts(Collection<Host> hosts);
	}
	
	public void addListener(SeedHostListener listener) {
		this.listener.set(listener);
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
	public <R> OperationResult<R> executeWithFailover(Operation<T, R> op,
			RetryPolicy retry) throws ConnectionException, OperationException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void shutdown() {
		// TODO Auto-generated method stub
		
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
