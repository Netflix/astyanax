package com.netflix.astyanax.connectionpool;

/**
 * Strategy used to calculate how much to back off for each subsequent attempt
 * to reconnect to a downed host
 * @author elandau
 *
 */
public interface RetryBackoffStrategy {
	public interface Instance {
		/**
		 * Return the next backoff delay in the strategy
		 * @return
		 */
		long nextDelay();

		/**
		 * nextTimeout should take into account that the host is being
		 * suspended.  
		 */
		void suspend();		
	};
	
	/**
	 * Create an instance of the strategy for a single host
	 * @return
	 */
	Instance createInstance();
}
