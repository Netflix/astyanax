package com.netflix.astyanax;

import java.util.List;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ColumnPath;
import com.netflix.astyanax.model.KeySlice;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.model.TokenRange;
import com.netflix.astyanax.query.ColumnFamilyQuery;

/**
 * Interface providing access to mutate and query columns from a cassandra keyspace.
 * 
 * @author elandau
 *
 */
public interface Keyspace {
    /** 
     * Returns keyspace name
     * @return
     */
    String getKeyspaceName();

    /**
     * Get a list of all tokens and their endpoints
     * @return
     * @throws ConnectionException 
     */
	public List<TokenRange> describeRing() throws ConnectionException;

	/**
	 * Prepare a batch mutation object.  It is possible to create multiple
	 * batch mutations and later merge them into a single mutation by calling
	 * mergeShallow on a batch mutation object.
	 * 
	 * @return
	 * @throws ConnectionException
	 */
	public MutationBatch prepareMutationBatch();
	
	/**
	 * Starting point for constructing a query.  From the column family the
	 * client can perform all 4 types of queries: get column, get key slice, 
	 * get key range and and index query.  
	 * 
	 * @param <K>
	 * @param <C>
	 * @param cf	Column family to be used for the query.  The key and column
	 * 				serializers in the ColumnFamily are automatically used while
	 * 				constructing the query and the response.
	 * @return
	 */
	public <K, C> ColumnFamilyQuery<K,C> prepareQuery(ColumnFamily<K, C> cf);
	
	/**
	 * Query for columns in a single row.  Must call .execute() to perform
	 * the query.
	 * @param <C>
	 * @param columnFamily
	 * @param rowKey
	 * @return
	 * @throws ConnectionException
	 */
	@Deprecated
	public <K,C> Query<K,C,ColumnList<C>> prepareGetRowQuery(ColumnFamily<K,?> columnFamily, Serializer<C> columnSerializer, K rowKey);
	
	/**
	 * Query all rows in the key slice. Must call .execute() to perform the query.
	 * @param <K>
	 * @param <C>
	 * @param columnFamily
	 * @param args
	 * @return
	 */
	@Deprecated
	public <K,C> Query<K,C,Rows<K,C>> prepareGetMultiRowQuery(ColumnFamily<K,?> columnFamily, Serializer<C> columnSerializer, KeySlice<K> keys);

	/**
	 * Query for a specific column within a row. Must call .execute() to perform the query.
	 * @param <K>
	 * @param <C>
	 * @param columnFamily
	 * @param path
	 * @return
	 */
	@Deprecated
	public <K,C> Query<K,C,Column<C>> prepareGetColumnQuery(ColumnFamily<K,?> columnFamily, K rowKey, ColumnPath<C> path);

	/**
	 * Mutation for a single counter.  Must call .execute() to perform the query.
	 * Counters can also be updated using the prepateMutation()
	 * 
	 * <pre>
	 * {@code
     *	  ColumnFamily<String, String> cf = new ColumnFamily<String, String>(
     *			"ColumnFamilyName",
     *			StringSerializer.get(),	// key serializer 
     *			StringSerializer.get(), // column name serializer
     *			ColumnType.STANDARD);
	 *    
	 *    Keyspace ks = ...
	 * 	  ks.prepareCounterMutation(someColumnFamily, rowKey, path, amount)
	 * 		  .execute();
	 * }
	 * </pre>
	 * @param <K>
	 * @param <C>
	 * @param columnFamily
	 * @param rowKey
	 * @param path
	 * @param amount - Amount to modify the counter.  Positive numbers increment.
	 * 				   Negative numbers decrement.
	 * @return Returns the mutation object.  The client must call execute() on 
	 * 			the mutation to perform the operation.
	 */
	public <K,C> CounterMutation<K, C> prepareCounterMutation(ColumnFamily<K,C> columnFamily, K rowKey, ColumnPath<C> path, long amount);
	
	/**
	 * Mutation for a single column
	 * @param <K>
	 * @param <C>
	 * @param columnFamily
	 * @return
	 */
	public <K,C> ColumnMutation prepareColumnMutation(ColumnFamily<K,C> columnFamily, K rowKey, C column);
	
	/**
	 * Terminate the keyspace and connection pool
	 */
	void shutdown();

	/**
	 * Start any threads or other tasks within the Keyspace
	 */
	void start();
}
