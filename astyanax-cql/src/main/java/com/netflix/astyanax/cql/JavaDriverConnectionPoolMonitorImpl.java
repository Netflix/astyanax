package com.netflix.astyanax.cql;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistryListener;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.Cluster;
import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.HostStats;

public class JavaDriverConnectionPoolMonitorImpl implements ConnectionPoolMonitor {

	private final AtomicReference<Cluster> cluster = new AtomicReference<Cluster>();
	
	private MetricRegistryListener metricsRegListener = new MetricRegistryListener(){

		@Override
		public void onGaugeAdded(String name, Gauge<?> gauge) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void onGaugeRemoved(String name) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void onCounterAdded(String name, Counter counter) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void onCounterRemoved(String name) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void onHistogramAdded(String name, Histogram histogram) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void onHistogramRemoved(String name) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void onMeterAdded(String name, Meter meter) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void onMeterRemoved(String name) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void onTimerAdded(String name, Timer timer) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void onTimerRemoved(String name) {
			// TODO Auto-generated method stub
			
		}};
	
	public JavaDriverConnectionPoolMonitorImpl() {	
	}
	
	public JavaDriverConnectionPoolMonitorImpl withJavaDriverMetricsRegistry(MetricRegistryListener metricsRegListener){
		this.metricsRegListener = metricsRegListener;
		return this;
	}
	
	public MetricRegistryListener getMetricsRegistryListener(){
		return metricsRegListener;
	}
	
    /**
     * Returns the number of Cassandra hosts currently known by the driver (that is 
     * whether they are currently considered up or down).
     *
     * @return the number of Cassandra hosts currently known by the driver.
     */
	@Override
	public long getHostCount() {
		return cluster.get().getMetrics().getKnownHosts().getValue();
	}

    /**
     * Returns the number of Cassandra hosts the driver is currently connected to
     * (that is have at least one connection opened to).
     *
     * @return the number of Cassandra hosts the driver is currently connected to.
     */
	@Override
	public long getHostActiveCount() {
		return cluster.get().getMetrics().getConnectedToHosts().getValue();
	}

    /**
     * Returns the total number of currently opened connections to Cassandra hosts.
     *
     * @return The total number of currently opened connections to Cassandra hosts.
     */
	public long getNumOpenConnections() {
		return cluster.get().getMetrics().getOpenConnections().getValue();
	}

    /**
     * Returns the number of connection to Cassandra nodes errors.
     * <p>
     * This represents the number of times that a request to a Cassandra node
     * has failed due to a connection problem. This thus also corresponds to
     * how often the driver had to pick a fallback host for a request.
     * <p>
     * You can expect a few connection errors when a Cassandra node fails
     * (or is stopped) ,but if that number grows continuously you likely have
     * a problem.
     *
     * @return the number of connection to Cassandra nodes errors.
     */
	@Override
	public long getConnectionCreateFailedCount() {
		return cluster.get().getMetrics().getErrorMetrics().getConnectionErrors().getCount();
	}

    /**
     * Returns the number of write requests that returned a timeout (independently
     * of the final decision taken by the {@link com.datastax.driver.core.policies.RetryPolicy}).
     *
     * @return the number of write timeout.
     */
    public long getWriteTimeouts() {
        return cluster.get().getMetrics().getErrorMetrics().getWriteTimeouts().getCount();
    }

    /**
     * Returns the number of read requests that returned a timeout (independently
     * of the final decision taken by the {@link com.datastax.driver.core.policies.RetryPolicy}).
     *
     * @return the number of read timeout.
     */
    public long getReadTimeouts() {
        return cluster.get().getMetrics().getErrorMetrics().getReadTimeouts().getCount();
    }

    /**
     * Returns the number of requests that returned errors not accounted for by
     * another metric. This includes all types of invalid requests.
     *
     * @return the number of requests errors not accounted by another
     * metric.
     */
	@Override
	public long getBadRequestCount() {
		return cluster.get().getMetrics().getErrorMetrics().getOthers().getCount();
	}

    /**
     * Returns the number of requests that returned an unavailable exception
     * (independently of the final decision taken by the 
     * {@link com.datastax.driver.core.policies.RetryPolicy}).
     *
     * @return the number of unavailable exceptions.
     */
	@Override
	public long notFoundCount() {
		return cluster.get().getMetrics().getErrorMetrics().getUnavailables().getCount();
	}

    /**
     * Returns the number of times a request was ignored
     * due to the {@link com.datastax.driver.core.policies.RetryPolicy}, for
     * example due to timeouts or unavailability.
     *
     * @return the number of times a request was ignored due to the
     * {@link com.datastax.driver.core.policies.RetryPolicy}.
     */
	@Override
	public long getSocketTimeoutCount() {
		return cluster.get().getMetrics().getErrorMetrics().getIgnores().getCount();
	}

    /**
     * Returns the number of times a request was retried due to the
     * {@link com.datastax.driver.core.policies.RetryPolicy}.
     *
     * @return the number of times a requests was retried due to the 
     * {@link com.datastax.driver.core.policies.RetryPolicy}.
     */
	@Override
	public long getUnknownErrorCount() {
		return cluster.get().getMetrics().getErrorMetrics().getRetries().getCount();
	}

	@Override
	public void incOperationFailure(Host host, Exception reason) {
		// TODO Auto-generated method stub

	}

	@Override
	public long getOperationFailureCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void incFailover(Host host, Exception reason) {
		// TODO Auto-generated method stub

	}

	@Override
	public long getFailoverCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void incOperationSuccess(Host host, long latency) {
		// TODO Auto-generated method stub

	}

	@Override
	public long getOperationSuccessCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void incConnectionCreated(Host host) {
		// TODO Auto-generated method stub

	}

	@Override
	public long getConnectionCreatedCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void incConnectionClosed(Host host, Exception reason) {
		// TODO Auto-generated method stub

	}

	@Override
	public long getConnectionClosedCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void incConnectionCreateFailed(Host host, Exception reason) {
		// TODO Auto-generated method stub

	}

	@Override
	public void incConnectionBorrowed(Host host, long delay) {
		// TODO Auto-generated method stub

	}

	@Override
	public long getConnectionBorrowedCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getConnectionReturnedCount() {
		return 0;
	}
	
	@Override
	public void incConnectionReturned(Host host) {
		// TODO Auto-generated method stub

	}

	@Override
	public long getPoolExhaustedTimeoutCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getOperationTimeoutCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getNoHostCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getInterruptedCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getTransportErrorCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getHostAddedCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getHostRemovedCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getHostDownCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void onHostAdded(Host host, HostConnectionPool<?> pool) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onHostRemoved(Host host) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onHostDown(Host host, Exception reason) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onHostReactivated(Host host, HostConnectionPool<?> pool) {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<Host, HostStats> getHostStats() {
		// TODO Auto-generated method stub
		return null;
	}

}
