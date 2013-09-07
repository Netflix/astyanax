package com.netflix.astyanax.cql.reads;

import java.nio.ByteBuffer;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.netflix.astyanax.model.ByteBufferRange;

public class CqlRangeImpl<T> implements ByteBufferRange {
	
	private final String columnName;
	private final T start;
    private final T end;
    private final int limit;
    private final boolean reversed;

    public CqlRangeImpl(String columnName, T start, T end, int limit, boolean reversed) {
    	this.columnName = columnName;
        this.start = start;
        this.end = end;
        this.limit = limit;
        this.reversed = reversed;
    }

    @Override
    public ByteBuffer getStart() {
		throw new NotImplementedException();
    }

    @Override
    public ByteBuffer getEnd() {
		throw new NotImplementedException();
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

}