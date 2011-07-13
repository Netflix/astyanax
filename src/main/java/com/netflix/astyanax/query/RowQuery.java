package com.netflix.astyanax.query;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ColumnPath;

/**
 * Interface to narrow down the path and column slices within a query after the
 * keys were seleted using the ColumnFamilyQuery.
 * 
 * @author elandau
 *
 * @param <K>
 * @param <C>
 */
public interface RowQuery<K, C> {
	/**
	 * Specify the path to a single column (either Standard or Super).  
	 * Notice that the sub column type and serializer will be used now.
	 * @param <C2>
	 * @param path
	 * @return
	 */
	ColumnQuery<C> getColumn(C column);

	/**
	 * Specify a non-contiguous set of columns to retrieve.
	 * @param columns
	 * @return
	 */
	RowQuery<K, C> withColumnSlice(C... columns);

	/**
	 * Specify a range of columns to return.  
	 * @param startColumn	First column in the range
	 * @param endColumn		Last column in the range
	 * @param reversed		True if the order should be reversed.  Note that for
	 * 						reversed, startColumn should be greater than endColumn.
	 * @param count			Maximum number of columns to return (similar to SQL LIMIT)
	 * @return
	 */
	RowQuery<K, C> withColumnRange(C startColumn, C endColumn, boolean reversed, int count);
	
	OperationResult<ColumnList<C>> execute() throws ConnectionException;
}
