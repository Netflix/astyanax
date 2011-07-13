package com.netflix.astyanax.query;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Rows;

public interface IndexQuery<K,C> {
	/**
	 * Limit the number of rows in the response
	 * @param count
	 * @return
	 */
	IndexQuery<K,C> setLimit(int count);

	/**
	 * ?
	 * @param key
	 * @return
	 */
	IndexQuery<K,C> setStartKey(K key);
	
	/**
	 * Add an expression (EQ, GT, GTE, LT, LTE) to the clause.
	 * Expressions are inherently ANDed
	 * @return
	 */
	IndexColumnExpression<K,C> addExpression();
	
	OperationResult<Rows<K,C>> execute() throws ConnectionException;

	IndexQuery<K, C> withColumnSlice(C... columns);

	IndexQuery<K, C> withColumnRange(C startColumn, C endColumn,
			boolean reversed, int count);
	
}
