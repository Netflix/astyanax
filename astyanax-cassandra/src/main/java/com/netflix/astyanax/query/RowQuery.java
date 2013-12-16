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
import com.netflix.astyanax.RowCopier;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ColumnSlice;

/**
 * Interface to narrow down the path and column slices within a query after the
 * keys were seleted using the ColumnFamilyQuery.
 * 
 * @author elandau
 * 
 * @param <K>
 * @param <C>
 */
public interface RowQuery<K, C> extends Execution<ColumnList<C>> {
    /**
     * Specify the path to a single column (either Standard or Super). Notice
     * that the sub column type and serializer will be used now.
     * 
     * @param <C2>
     * @param path
     */
    ColumnQuery<C> getColumn(C column);

    /**
     * Specify a non-contiguous set of columns to retrieve.
     * 
     * @param columns
     */
    RowQuery<K, C> withColumnSlice(Collection<C> columns);

    /**
     * Specify a non-contiguous set of columns to retrieve.
     * 
     * @param columns
     */
    RowQuery<K, C> withColumnSlice(C... columns);

    /**
     * Use this when your application caches the column slice.
     * 
     * @param slice
     */
    RowQuery<K, C> withColumnSlice(ColumnSlice<C> columns);

    /**
     * Specify a range of columns to return. Use this for simple ranges for
     * non-composite column names. For Composite column names use
     * withColumnRange(ByteBufferRange range) and the
     * AnnotatedCompositeSerializer.buildRange()
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
    RowQuery<K, C> withColumnRange(C startColumn, C endColumn, boolean reversed, int count);

    /**
     * Specify a range and provide pre-constructed start and end columns. Use
     * this with Composite columns
     * 
     * @param startColumn
     * @param endColumn
     * @param reversed
     * @param count
     */
    RowQuery<K, C> withColumnRange(ByteBuffer startColumn, ByteBuffer endColumn, boolean reversed, int count);

    /**
     * Specify a range of composite columns. Use this in conjunction with the
     * AnnotatedCompositeSerializer.buildRange().
     * 
     * @param range
     */
    RowQuery<K, C> withColumnRange(ByteBufferRange range);

    @Deprecated
    /**
     * Use autoPaginate instead
     */
    RowQuery<K, C> setIsPaginating();

    /**
     * When used in conjunction with a column range this will call subsequent
     * calls to execute() to get the next block of columns.
     */
    RowQuery<K, C> autoPaginate(boolean enabled);

    /**
     * Copy the results of the query to another column family
     * 
     * @param columnFamily
     * @param otherRowKey
     */
    RowCopier<K, C> copyTo(ColumnFamily<K, C> columnFamily, K rowKey);

    /**
     * Returns the number of columns in the response without returning any data
     * @throws ConnectionException
     */
    ColumnCountQuery getCount();
}
