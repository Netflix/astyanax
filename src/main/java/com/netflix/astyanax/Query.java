package com.netflix.astyanax;

import com.netflix.astyanax.connectionpool.FutureOperationResult;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnPath;
import com.netflix.astyanax.model.ColumnSlice;
import com.netflix.astyanax.model.ConsistencyLevel;

/**
 * Base interface for performing a query.  This interface provides additional
 * customization to the getXXX query calls on CassandraClient.  
 * @author elandau
 *
 * @param <K>	keyType
 * @param <C> 	ColumnType
 * @param <R>	ResponseType
 */
public interface Query<K, C, R> {
	/**
	 * Execute the query and return the result, including statistics
	 * @return
	 * @throws ConnectionException 
	 */
	OperationResult<R> execute() throws ConnectionException;
	
	/**
	 * Execute the query asynchronously.  Return a FutureOperationResult which
	 * acts exactly as a standard Java Future.
	 * @return
	 */
	FutureOperationResult<R> executeAsync();
	
	/**
	 * Set the consistency level for this query
	 * @param consistencyLevel
	 */
	Query<K,C,R> setConsistencyLevel(ConsistencyLevel consistencyLevel);
	
	/**
	 * Set the timeout for this query.  Set a high timeout when expecting a large
	 * result set such as when querying a KeySlice.  
	 * @param timeout In milliseconds
	 */
	Query<K,C,R> setTimeout(long timeout);
	
	/**
	 * Set the path to a super column or column
	 * @param path
	 */
	Query<K,C,R> setColumnPath(ColumnPath<C> path);
	
	/**
	 * Set the slice of columns to be returned
	 * @param slice
	 */
	Query<K,C,R> setColumnSlice(ColumnSlice<C> slice);
}
