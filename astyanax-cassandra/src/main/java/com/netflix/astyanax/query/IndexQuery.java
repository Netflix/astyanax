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
import java.util.Collection;

import com.netflix.astyanax.Execution;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.ColumnSlice;
import com.netflix.astyanax.model.Rows;

public interface IndexQuery<K, C> extends Execution<Rows<K, C>> {
    /**
     * Limit the number of rows in the response
     * 
     * @param count
     * @deprecated Use setRowLimit instead
     */
    @Deprecated
    IndexQuery<K, C> setLimit(int count);

    /**
     * Limits the number of rows returned
     * 
     * @param count
     */
    IndexQuery<K, C> setRowLimit(int count);

    /**
     * @param key
     */
    IndexQuery<K, C> setStartKey(K key);

    /**
     * Add an expression (EQ, GT, GTE, LT, LTE) to the clause. Expressions are
     * inherently ANDed
     */
    IndexColumnExpression<K, C> addExpression();

    /**
     * Add a set of prepare index expressions.
     * 
     * @param expressions
     */
    IndexQuery<K, C> addPreparedExpressions(Collection<PreparedIndexExpression<K, C>> expressions);

    /**
     * Specify a non-contiguous set of columns to retrieve.
     * 
     * @param columns
     */
    IndexQuery<K, C> withColumnSlice(C... columns);

    /**
     * Specify a non-contiguous set of columns to retrieve.
     * 
     * @param columns
     */
    IndexQuery<K, C> withColumnSlice(Collection<C> columns);

    /**
     * Use this when your application caches the column slice.
     * 
     * @param slice
     */
    IndexQuery<K, C> withColumnSlice(ColumnSlice<C> columns);

    /**
     * Specify a range of columns to return.
     * 
     * @param startColumn
     *            First column in the range
     * @param endColumn
     *            Last column in the range
     * @param reversed
     *            True if the order should be reversed. Note that for reversed,
     *            startColumn should be greater than endColumn.
     * @param count
     *            Maximum number of columns to return (similar to SQL LIMIT)
     */
    IndexQuery<K, C> withColumnRange(C startColumn, C endColumn, boolean reversed, int count);

    /**
     * Specify a range and provide pre-constructed start and end columns. Use
     * this with Composite columns
     * 
     * @param startColumn
     * @param endColumn
     * @param reversed
     * @param count
     */
    IndexQuery<K, C> withColumnRange(ByteBuffer startColumn, ByteBuffer endColumn, boolean reversed, int count);

    /**
     * Specify a range of composite columns. Use this in conjunction with the
     * AnnotatedCompositeSerializer.buildRange().
     * 
     * @param range
     */
    IndexQuery<K, C> withColumnRange(ByteBufferRange range);

    /**
     * @deprecated autoPaginateRows()
     */
    IndexQuery<K, C> setIsPaginating();

    /**
     * Automatically sets the next start key so that the next call to execute
     * will fetch the next block of rows
     */
    IndexQuery<K, C> autoPaginateRows(boolean autoPaginate);

}
