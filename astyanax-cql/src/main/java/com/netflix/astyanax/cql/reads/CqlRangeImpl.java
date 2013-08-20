package com.netflix.astyanax.cql.reads;

import java.nio.ByteBuffer;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.netflix.astyanax.model.ByteBufferRange;

public class CqlRangeImpl implements ByteBufferRange {
	
	private final String columnName;
	private final Object start;
    private final Object end;
    private final int limit;
    private final boolean reversed;

    public CqlRangeImpl(String columnName, Object start, Object end, int limit, boolean reversed) {
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
    
    public Object getCqlStart() {
		return start;
    }

    public Object getCqlEnd() {
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