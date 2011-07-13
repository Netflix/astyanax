package com.netflix.astyanax.model;

import java.util.Collection;

/**
 * Definition for a sub set of slices.  A subset can either be a fixed set
 * of columns a range of ordered columns.  The slice defines the sub set of
 * columns at the ColumnPath position within the row.
 * 
 * @author elandau
 *
 * @param <C>
 */
public class ColumnSlice<C> {
	private Collection<C> columns;
	
	// - or -
	
	private C startColumn;
	private C endColumn;
	private boolean reversed = false;
	private int limit = Integer.MAX_VALUE;
	
	public ColumnSlice(Collection<C> columns) {
		this.columns = columns;
	}
	
	public ColumnSlice(C startColumn, C endColumn) {
		this.startColumn = startColumn;
		this.endColumn = endColumn;
	}

	public ColumnSlice<C> setLimit(int limit) {
		this.limit = limit;
		return this;
	}
	
	public ColumnSlice<C> setReversed(boolean value) {
		this.reversed = value;
		return this;
	}
	
	public Collection<C> getColumns() { 
		return columns;
	}
	
	public C getStartColumn() {
		return startColumn;
	}
	
	public C getEndColumn() {
		return endColumn;
	}
	
	public boolean getReversed() {
		return reversed;
	}
	
	public int getLimit() {
		return limit;
	}
}
