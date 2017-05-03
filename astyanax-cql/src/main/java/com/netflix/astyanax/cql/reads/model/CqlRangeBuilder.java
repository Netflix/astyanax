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

/**
 * Helpful class that tracks the state for building a {@link ColumnSlice} query using column range specification. 
 * 
 * @author poberai
 *
 * @param <T>
 */
public class CqlRangeBuilder<T> {
	
    private T start = null;
    private T end = null;
    private int limit = -1;
    private boolean reversed = false;
    private int fetchSize = -1;

    private String columnName = "column1";

    public CqlRangeBuilder<T> withRange(CqlRangeImpl<T> oldRange) {
    	if (oldRange != null) {
    		this.start = oldRange.getCqlStart();
    		this.end = oldRange.getCqlEnd();
    		this.limit = oldRange.getLimit();
    		this.reversed = oldRange.isReversed();
    		this.fetchSize = oldRange.getFetchSize();
    	}
        return this;
    }

    public CqlRangeBuilder<T> setLimit(int count) {
        this.limit = count;
        return this;
    }

    public int getLimit() {
    	return this.limit;
    }
    
    public CqlRangeBuilder<T> setReversed(boolean reversed) {
        this.reversed = reversed;
        return this;
    }
    
    public boolean getReversed() {
    	return this.reversed;
    }

    public CqlRangeBuilder<T> setStart(T value) {
        start = value;
        return this;
    }

    public T getStart() {
    	return this.start;
    }
    
    public CqlRangeBuilder<T> setEnd(T value) {
        end = value;
        return this;
    }

    public T getEnd() {
    	return this.end;
    }
    
    public CqlRangeBuilder<T> setColumn(String name) {
    	this.columnName = name;
    	return this;
    }

    public String getColumn() {
    	return this.columnName;
    }
    
    public CqlRangeBuilder<T> setFetchSize(int count) {
        this.fetchSize = count;
        return this;
    }

    public int getFetchSize() {
    	return this.fetchSize;
    }

    public CqlRangeImpl<T> build() {
    	return new CqlRangeImpl<T>(columnName, start, end, limit, reversed, fetchSize);
    }
}
