/*******************************************************************************
 * Copyright 2011 Netflix
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.netflix.astyanax.query;

import java.nio.ByteBuffer;

import com.netflix.astyanax.Execution;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.ColumnSlice;
import com.netflix.astyanax.model.Rows;

public interface IndexQuery<K,C> extends Execution<Rows<K,C>> {
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
	
	/**
	 * Specify a non-contiguous set of columns to retrieve.
	 * @param columns
	 * @return
	 */
	IndexQuery<K, C> withColumnSlice(C... columns);

	/**
	 * Use this when your application caches the column slice.
	 * @param slice
	 * @return
	 */
	IndexQuery<K, C> withColumnSlice(ColumnSlice<C> columns);
	
	/**
	 * Specify a range of columns to return.  
	 * @param startColumn	First column in the range
	 * @param endColumn		Last column in the range
	 * @param reversed		True if the order should be reversed.  Note that for
	 * 						reversed, startColumn should be greater than endColumn.
	 * @param count			Maximum number of columns to return (similar to SQL LIMIT)
	 * @return
	 */
	IndexQuery<K, C> withColumnRange(C startColumn, C endColumn,
			boolean reversed, int count);
	
	/**
	 * Specify a range and provide pre-constructed start and end columns.
	 * Use this with Composite columns
	 * 
	 * @param startColumn
	 * @param endColumn
	 * @param reversed
	 * @param count
	 * @return
	 */
	IndexQuery<K, C> withColumnRange(ByteBuffer startColumn, ByteBuffer endColumn, boolean reversed, int count);
	
	/**
	 * Specify a range of composite columns.  Use this in conjunction with the
	 * AnnotatedCompositeSerializer.buildRange().
	 * 
	 * @param range
	 * @return
	 */
	IndexQuery<K, C> withColumnRange(ByteBufferRange range);
}
