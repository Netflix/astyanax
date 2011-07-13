package com.netflix.astyanax;

import java.util.Collection;

import com.netflix.astyanax.connectionpool.FutureOperationResult;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;

/**
 * Batch mutator which operates at the row level assuming the hierarchy: 
 * 
 * 	RowKey -> ColumnFamily -> Mutation.
 * 
 * This hierarchy serves two purposes.  First, it makes it possible to perform
 * multiple operations on the same row without having to repeat specifying the
 * row key.  Second, it mirrors the underlying Thrift data structure which 
 * averts unnecessary operations to convert from one data structure to another. 
 * 
 * The mutator is not thread safe
 * 
 * Example:
 * <pre>
 * {@code
 * 	
 * 	ColumnFamily<String, String> cf = 
 * 		AFactory.makeColumnFamily(
 * 				"COLUMN_FAMILY_NAME", 	// Name of CF in Cassandra
 * 				StringSerializer.get(), // Row key serializer (implies string type)
 * 				StringSerializer.get(), // Column name serializer (implies string type)
 * 				ColumnType.STANDARD);	// This is a standard row
 *
 * 	// Create a batch mutation
 * 	RowMutationBatch m = keyspace.prepareMutationBatch();
 *
 *  // Start mutate a column family for a specific row key 
 *  ColumnFamilyMutation<String> cfm = m.row(cfSuper, "UserId")
 *	   .putColumn("Address", "976 Elm St.")
 *     .putColumn("Age", 50)
 *     .putColumn("Gender", "Male");
 *  
 *  // To delete a row
 *  m.row(cfSuper, "UserId").delete();
 *  
 *  // Finally, execute the query
 *  m.execute();
 *  
 * }
 * </pre>
 * @author elandau
 *
 * @param <K>
 */
public interface MutationBatch {
	/**
	 * Execute the mutation and return info about its execution.  
	 * If successful, all the mutations are cleared and new mutations may be 
	 * created.  Any previously acquired ColumnFamilyMutations are no longer
	 * valid and should be discarded.
	 * 
	 * @return  No data is actually returned after a mutation is executed, hence 
	 * 			the Void return value type.
	 * @throws ConnectionException
	 */
	OperationResult<Void> execute() throws ConnectionException;
	
	/**
	 * Execute the mutation asynchronously.  
	 * 
	 * @return
	 * @throws ConnectionException
	 */
	FutureOperationResult<Void> executeAsync() throws ConnectionException;
	
	/**
	 * Mutate a row.  The ColumnFamilyMutation is only valid until execute()
	 * or discardMutations is called.  
	 * 
	 * @param rowKey
	 * @return
	 */
	<K, C> ColumnListMutation<C> withRow(ColumnFamily<K,C> columnFamily, K rowKey);

	/**
	 * Delete the row for all the specified column families
	 * @param columnFamilies
	 */
	<K> void deleteRow(Collection<ColumnFamily<K, ?>> columnFamilies, K rowKey);

	/**
	 * Discard any pending mutations.  All previous references returned by
	 * row are now invalid.
	 */
	void discardMutations();

	/**
	 * Perform a shallow merge of mutations from another batch.
	 * @throws UnsupportedOperationException if the other mutation is of a different type
	 */
	void mergeShallow(MutationBatch other);
	
	/**
	 * Returns true if there are no rows in the mutation.  May return a false
	 * true if a row() was added by calling the above row() method but no 
	 * mutations were created.
	 * @return
	 */
	boolean isEmpty();
	
	/**
	 * Returns the number of rows being mutated
	 * @return
	 */
	int getRowCount();

	/**
	 * Set the consistency level for this mutation
	 * @param consistencyLevel
	 */
	MutationBatch setConsistencyLevel(ConsistencyLevel consistencyLevel);
	
	/**
	 * Set the timeout for this mutation.  Set a high timeout when mutating
	 * a large number of rows
	 * @param timeout In milliseconds
	 */
	MutationBatch setTimeout(long timeout);
	

}
