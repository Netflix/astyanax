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
package com.netflix.astyanax.cql.reads.model;

import java.nio.ByteBuffer;

import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.ColumnSlice;
import com.netflix.astyanax.query.RowQuery;

/**
 * Impl for {@link ByteBufferRange} that tracks the individual components of a {@link ColumnSlice} when using a column range
 * specification. 
 * 
 * Users of such queries (columns slices with column ranges) can use this class when performing using the {@link RowQuery}
 * 
 * @author poberai
 *
 * @param <T>
 */
public class CqlRangeImpl<T> implements ByteBufferRange {
	
	private final String columnName;
	private final T start;
    private final T end;
    private final int limit;
    private final boolean reversed;
    private int fetchSize = -1;

    public CqlRangeImpl(String columnName, T start, T end, int limit, boolean reversed, int fetchSize) {
    	this.columnName = columnName;
        this.start = start;
        this.end = end;
        this.limit = limit;
        this.reversed = reversed;
        this.fetchSize = fetchSize;
    }

    @Override
    public ByteBuffer getStart() {
		throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public ByteBuffer getEnd() {
		throw new UnsupportedOperationException("Operation not supported");
    }

    public String getColumnName() {
    	return columnName;
    }
    
    public T getCqlStart() {
		return start;
    }

    public T getCqlEnd() {
		return end;
    }

    @Override
    public boolean isReversed() {
        return reversed;
    }

    @Override
    public int getLimit() {
        return limit;
    }
    
    public int getFetchSize() {
    	return fetchSize;
    }
    
    public void setFetchSize(int size) {
    	fetchSize = size;
    }
}