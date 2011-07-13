package com.netflix.astyanax.connectionpool;

/**
 * Abstraction for a service used to retry connecting to a host.  Hosts are placed
 * in this service after they have been identified to be down and were removed
 * from the list of available hosts in the connection pool.
 * @author elandau
 *
 * @param <CL>
 */
public interface HostRetryService {
	public static interface ReconnectCallback {
		public void onReconnected(Host host);
	}
	
	/**
	 * Shut down the reconnect service and any attempts to reconnect
	 */
	void shutdown();
	
	/**
	 * Add a host to the service
	 * @param host
	 */
	void addHost(Host host, ReconnectCallback callback);

	/**
	 * Stop retrying the host
	 * @param host
	 */
	void removeHost(Host host);
}
