package com.netflix.astyanax.cql.reads;

import java.util.Collection;

import com.netflix.astyanax.model.ColumnSlice;

@SuppressWarnings("unchecked")
public class CqlColumnSlice<C> extends ColumnSlice<C> {
	
	private CqlRangeImpl cqlRange;
	private Collection<C> cqlColumns;
	
	public CqlColumnSlice() {
		super(null, null);
	}

	public CqlColumnSlice(C startColumn, C endColumn) {
		super(null, null);
	}
	
	public CqlColumnSlice(CqlRangeImpl cqlRange) {
		super(null, null);
		this.cqlRange = cqlRange;
	}

	public CqlColumnSlice(Collection<C> columns) {
		super(null, null);
		this.cqlColumns = columns;
	}

	public CqlColumnSlice(ColumnSlice<C> columnSlice) {
		super(null, null);
		
		if (columnSlice instanceof CqlColumnSlice<?>) {
			initFrom(((CqlColumnSlice<?>)columnSlice));
		} else {
			
			if (columnSlice.getColumns() != null) {
				 this.cqlColumns = columnSlice.getColumns();
				 this.cqlRange = null;
			} else {
				// this is where the consumer is using the old style range query using the same code i.e no column name specified.
				// in this case we must assume the columnName = 'column1' which is the default chosen by CQL3
				this.cqlColumns = null;
				this.cqlRange = new CqlRangeBuilder<C>()
											.setColumn("column1")
											.setStart(columnSlice.getStartColumn())
											.setEnd(columnSlice.getEndColumn())
											.setReversed(columnSlice.getReversed())
											.setLimit(columnSlice.getLimit())
											.build();
			}
		}
		
	}

	public CqlColumnSlice(CqlColumnSlice<C> cqlColumnSlice) {
		super(null, null);
		initFrom(cqlColumnSlice);
	}
	
	private void initFrom(CqlColumnSlice<?> cqlColumnSlice) {
		this.cqlColumns = (Collection<C>) cqlColumnSlice.cqlColumns;
		this.cqlRange = cqlColumnSlice.cqlRange;
	}

	@Override
	public ColumnSlice<C> setLimit(int limit) {
		throw new IllegalStateException();
	}

	@Override
	public ColumnSlice<C> setReversed(boolean value) {
		throw new IllegalStateException();
	}

	public String getColumnName() {
		return cqlRange.getColumnName();
	}

	@Override
	public Collection<C> getColumns() {
		return cqlColumns;
	}

	@Override
	public C getStartColumn() {
		return (cqlRange != null) ? (C) cqlRange.getCqlStart() : null;
	}

	@Override
	public C getEndColumn() {
		return (cqlRange != null) ? (C) cqlRange.getCqlEnd() : null;
	}

	@Override
	public boolean getReversed() {
		return (cqlRange != null ) ? cqlRange.isReversed() : false;
	}

	@Override
	public int getLimit() {
		return (cqlRange != null ) ? cqlRange.getLimit() : -1;
	}
	
	public boolean isColumnSelectQuery() {
		return (this.cqlColumns != null);
	}

	public boolean isRangeQuery() {
		
		if (isColumnSelectQuery()) {
			return false;
		}
		
		if (cqlRange != null) {
			return true;
		}
		
		return false;
	}
	
	
	public boolean isSelectAllQuery() {
		return (!isColumnSelectQuery() && !isRangeQuery());
	}
	
	public static enum QueryType {
		SELECT_ALL, COLUMN_COLLECTION, COLUMN_RANGE;
	}
	
	public QueryType getQueryType() {
		if (isSelectAllQuery()) {
			return QueryType.SELECT_ALL;
		} else if (isRangeQuery()) {
			return QueryType.COLUMN_RANGE;
		} else {
			return QueryType.COLUMN_COLLECTION;
		}
	}
}
