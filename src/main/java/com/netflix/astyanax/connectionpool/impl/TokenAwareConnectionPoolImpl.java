package com.netflix.astyanax.connectionpool.impl;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.astyanax.connectionpool.Connection;
import com.netflix.astyanax.connectionpool.ConnectionFactory;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NoAvailableHostsException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.connectionpool.exceptions.PoolTimeoutException;
import com.netflix.astyanax.connectionpool.exceptions.TimeoutException;
import com.netflix.astyanax.connectionpool.exceptions.TransportException;
import com.netflix.astyanax.connectionpool.exceptions.UnknownException;
import com.netflix.logging.ILog;
import com.netflix.logging.LogManager;

/**
 * Connection pool that partitions connections by the hosts which own the token
 * being operated on.  When a token is not available or an operation is known
 * to span multiple tokens (such as a batch mutate or an index query) host pools
 * are picked using round robin.
 * 
 * This implementation takes an optimistic approach which is optimized for 
 * a well functioning ring with all nodes up and keeps downed hosts in the 
 * internal data structures.
 * 
 * @author elandau
 *
 * @param <CL>
 */
public class TokenAwareConnectionPoolImpl<CL> extends RoundRobinConnectionPoolImpl<CL> {

    private static ILog logger = LogManager.getLogger(TokenAwareConnectionPoolImpl.class);
    
	/**
	 * Sorted list of TokenPartition, one per token.  The list is updated 
	 * frequently via a describe ring. This list is used to determine which 
	 * host owns a token so that a request may be made to that host directly
	 */
	private AtomicReference<ArrayList<TokenPartition<CL>>> ring = 
		new AtomicReference<ArrayList<TokenPartition<CL>>>();
	
	/**
	 * A token partition associating a start token with a connection pool.
	 * @author elandau
	 *
	 * @param <CL>
	 */
	static class TokenPartition<CL> implements Comparable<TokenPartition<CL>> {
		BigInteger token;
		List<HostConnectionPool<CL>> pools;
		
		TokenPartition(BigInteger token, List<HostConnectionPool<CL>> pools) {
			this.token = token;
			this.pools = pools;
		}
		
		@Override
		public int compareTo(TokenPartition<CL> other) {
			return token.compareTo(other.token);
		}
	}
	
	/**
	 * Comparator used to find the connection pool to the host which owns a 
	 * specific token
	 */
	@SuppressWarnings("rawtypes")
	private Comparator tokenComparator = new Comparator() {
		@SuppressWarnings("unchecked")
		@Override
		public int compare(Object arg0, Object arg1) {
			TokenPartition<CL> partition = (TokenPartition<CL>)arg0;
			BigInteger token = (BigInteger)arg1;
			return partition.token.compareTo(token);
		}
	};
	
	public TokenAwareConnectionPoolImpl(ConnectionPoolConfiguration configuration, ConnectionFactory<CL> factory) {
		super(configuration, factory);
	}
	
	@Override
	public <R> Connection<CL> borrowConnection(Operation<CL, R> op)
			throws ConnectionException, OperationException {
		// First, get a copy of the partitions.  
		ArrayList<TokenPartition<CL>> partitions = this.ring.get();
		
		// Must have a token otherwise we default to the base class implementation
		BigInteger token = null;
		if (op != null)
			token = op.getKey();
		
		if (token == null || partitions == null) {
			// No token provided so use the default implementation
			return super.borrowConnection(op);
		}
		else {
			List<HostConnectionPool<CL>> partition = getPartition(partitions, token);
			for (HostConnectionPool<CL> pool : partition ) {
				try {
					return pool.borrowConnection(config.getMaxTimeoutWhenExhausted());
				}
				catch (ConnectionException e) {
					// TODO
				}
				finally {
					
				}
			}
			throw new NoAvailableHostsException("No available hosts in partition to process the request");
		}
	}

	/**
	 * Call the default implementation to add all hosts to the connection pool
	 * and remove any old hosts.  Then recreate the ring.
	 * 
	 * @param ring - Map of token to list of hosts
	 */
	@Override
	public void setHosts(Map<String, List<Host>> ring) {
		super.setHosts(ring);
		
		ArrayList<TokenPartition<CL>> partitions = new ArrayList<TokenPartition<CL>>(ring.size());
		
		for (Map.Entry<String, List<Host>> entry : ring.entrySet()) {
			if (entry.getValue() != null) {
				List<HostConnectionPool<CL>> pools = new ArrayList<HostConnectionPool<CL>>();
				for (Host host : entry.getValue()) {
					HostConnectionPool<CL> pool = activeHosts.get(host);
					if (pool != null) {
						pools.add(pool);
					}
				}
				
				partitions.add(
					new TokenPartition<CL>(
						new BigInteger(entry.getKey()), 
						pools));
			}
		}
		
		Collections.sort(partitions);
		this.ring.set(partitions);
	}
	
	@Override 
	protected void onHostDown(HostConnectionPool<CL> pool) {
		super.onHostDown(pool);
		rebuildPartitions();
	}
	
	@Override 
	protected void onHostUp(HostConnectionPool<CL> pool) {
		super.onHostUp(pool);
		rebuildPartitions();
	}
	
	/**
	 * Rebuild the partitions using the current partition state and only include
	 * hosts that are active.  Any hosts that are no longer active will not be
	 * included in the new partitions structure.
	 */
	public void rebuildPartitions() {
		ArrayList<TokenPartition<CL>> partitions = this.ring.get();
		ArrayList<TokenPartition<CL>> newPartitions = new ArrayList<TokenPartition<CL>>(partitions.size());
		for (TokenPartition<CL> partition : partitions) {
			ArrayList<HostConnectionPool<CL>> newPools = new ArrayList<HostConnectionPool<CL>>();
			
			for (HostConnectionPool<CL> pool : partition.pools) {
				HostConnectionPool<CL> newPool = activeHosts.get(pool.getHost());
				if (newPool != null) {
					newPools.add(newPool);
				}
			}
			
			TokenPartition<CL> newPartition = new TokenPartition<CL>(
					partition.token, newPools);
			newPartitions.add(newPartition);
		}
		
		Collections.sort(partitions);
		this.ring.compareAndSet(partitions, newPartitions);
	}
	
	@Override
	public <R> OperationResult<R> executeWithFailover(Operation<CL, R> op)
			throws ConnectionException, OperationException {
		// First, get a copy of the partitions.  
		ArrayList<TokenPartition<CL>> partitions = this.ring.get();
		
		BigInteger token = null;
		if (op != null) {
			token = op.getKey();
		}
		
		// Must have a token otherwise we default to the base class implementation
		if (token == null || partitions == null) {
			return super.executeWithFailover(op);
		}

		// Only try to perform the operation on a replica
		List<HostConnectionPool<CL>> pools = getPartition(partitions, token);
		
		// Determine max failover values
		int size = pools.size();
		int failoverCount = this.failoverStrategy.getMaxRetries();
		if (failoverCount < 0) 
			failoverCount = size;
		
		int retryCount = this.exhaustedStrategy.getMaxRetries();
		if (retryCount < 0) {
			retryCount = size;
		}
		
		// Iterate through the pools trying to execute
		for (HostConnectionPool<CL> pool : pools) {
			// Try to get a connection
			Connection<CL> connection = null;
			try {
				connection = pool.borrowConnection(config.getMaxTimeoutWhenExhausted());
			}
			catch (ConnectionException e) {
				// These are failures trying to open a new connection
				if (e instanceof TimeoutException || e instanceof TransportException || e instanceof UnknownException) {
					if (this.badHostDetector.checkFailure(pool.getHost(), e)) {
						this.markHostAsDown(pool, e);
					}
				}
				
				if (!e.isRetryable())
					throw e;
				
				failoverCount--;
				retryCount--;
				if (failoverCount > 0) {
					this.monitor.incFailover();
				}
				else if (retryCount <= 0) {
					throw new PoolTimeoutException("Timed out trying to borrow a connection");
				}
				
				this.monitor.incBorrowRetry();
				continue;
			}
			
			// Now try to execute
			try {
				return connection.execute(op);
			}
			catch (ConnectionException e) {
				if (!e.isRetryable()) 
					throw e;
			}
			finally {
				returnConnection(connection);
			}
			
			// Apply the failover strategy
			if (--failoverCount > 0) {
				this.monitor.incFailover();
				if (this.failoverStrategy.getWaitTime() > 0) {
					try {
						Thread.sleep(this.failoverStrategy.getWaitTime());
					} 
					catch (InterruptedException e) {
						throw new TimeoutException("Interrupted sleeping between retries");
					}
				}
			}
			else {
				throw new TimeoutException("Operation failed too many times");
			}
		} while(true);
	}
	
	private List<HostConnectionPool<CL>> getPartition(List<TokenPartition<CL>> partitions, BigInteger token) {
		// Do a binary search to find the token partition which owns the
		// token.  We can get two responses here.  
		// 1.  The token is in the list in which case the response is the 
		//		index of the partition
		// 2.  The token is not in the list in which case the response is the
		//		index where the token would have been inserted into the list.
		//		We convert this index (which is negative) to the index of the
		//		previous position in the list.
		int j = Collections.binarySearch(partitions, token, tokenComparator);
		if (j < 0) {
			j = -j - 2;
		}
		
		return partitions.get(j % partitions.size()).pools;
	}
}
