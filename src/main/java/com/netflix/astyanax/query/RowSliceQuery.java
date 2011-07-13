package com.netflix.astyanax.query;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnPath;
import com.netflix.astyanax.model.Rows;

/**
 * Interface to narrow down the path and column slices within a query after the
 * keys were seleted using the ColumnFamilyQuery.
 */
public interface RowSliceQuery<K, C> {
	/**
	 * Specify a non-contiguous set of columns to retrieve.
	 * @param columns
	 * @return
	 */
	RowSliceQuery<K, C> withColumnSlice(C... columns);

	/**
	 * Specify a range of columns to return.  
	 * @param startColumn	First column in the range
	 * @param endColumn		Last column in the range
	 * @param reversed		True if the order should be reversed.  Note that for
	 * 						reversed, startColumn should be greater than endColumn.
	 * @param count			Maximum number of columns to return (similar to SQL LIMIT)
	 * @return
	 */
	RowSliceQuery<K, C> withColumnRange(C startColumn, C endColumn, boolean reversed, int count);
	
	/**
	 * Execute the row slice
	 * @return
	 * @throws ConnectionException
	 */
	OperationResult<Rows<K,C>> execute() throws ConnectionException;
}
