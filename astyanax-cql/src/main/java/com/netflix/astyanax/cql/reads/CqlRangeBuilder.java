package com.netflix.astyanax.cql.reads;

import com.google.common.base.Preconditions;

public class CqlRangeBuilder<T> {
	
    private T start = null;
    private T end = null;
    private int limit = -1;
    private boolean reversed = false;

    private String columnName = "column1";

    public CqlRangeBuilder<T> setLimit(int count) {
        Preconditions.checkArgument(count >= 0, "Invalid count in RangeBuilder : " + count);
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
    
    public CqlRangeImpl build() {
    	return new CqlRangeImpl(columnName, start, end, limit, reversed);
    }
}
