package com.netflix.astyanax.thrift;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.CounterSuperColumn;
import org.apache.cassandra.thrift.SuperColumn;

import com.google.common.base.Preconditions;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.exceptions.InvalidOperationException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;

/**
 * List of columns that can be either a list of super columns or standard
 * columns.
 * 
 * @author elandau
 *
 * @param <C>
 */
public class ThriftColumnOrSuperColumnListImpl<C> implements ColumnList<C> {
	private final List<ColumnOrSuperColumn> columns;
	private HashMap<C, ColumnOrSuperColumn> lookup;
	private final Serializer<C> colSer;
	
	public ThriftColumnOrSuperColumnListImpl(List<ColumnOrSuperColumn> columns,
			Serializer<C> colSer) {
        Preconditions.checkArgument(columns != null, "Columns must not be null");
        Preconditions.checkArgument(colSer!= null, "Serializer must not be null");
        
		this.columns = columns;
		this.colSer  = colSer; 
	}

	@Override
	public Iterator<Column<C>> iterator() {
		class IteratorImpl implements Iterator<Column<C>> {
			Iterator<ColumnOrSuperColumn> base;
			
			public IteratorImpl(Iterator<ColumnOrSuperColumn> base) {
				this.base = base;
			}
			
			@Override
			public boolean hasNext() {
				return base.hasNext();
			}

			@Override
			public Column<C> next() {
				ColumnOrSuperColumn column = base.next();
				if (column.isSetSuper_column()) {
					SuperColumn sc = column.getSuper_column();
					return new ThriftSuperColumnImpl<C>(
							colSer.fromBytes(sc.getName()), 
							sc.getColumns());
				}
				else if (column.isSetCounter_column()) {
					CounterColumn cc = column.getCounter_column();
					return new ThriftCounterColumnImpl<C>(
							colSer.fromBytes(cc.getName()),
							cc.getValue());
				}
				else if (column.isSetCounter_super_column()) {
					CounterSuperColumn cc = column.getCounter_super_column();
					return new ThriftCounterSuperColumnImpl<C>(
							colSer.fromBytes(cc.getName()),
							cc.getColumns());
				}
				else if (column.isSetColumn()) {
					org.apache.cassandra.thrift.Column c = column.getColumn();
					return new ThriftColumnImpl<C>(
							colSer.fromBytes(c.getName()), 
							c.getValue());
				}
				else {
					throw new RuntimeException("Unknwon column type");
				}
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException("Iterator is immutable");
			}
		}
		return new IteratorImpl(columns.iterator());
	}

	@Override
	public Column<C> getColumnByName(C columnName)  {
		ColumnOrSuperColumn column = getColumn(columnName);
		if (column == null) {
			return null;
		}
		else if (column.isSetColumn()) {
			return new ThriftColumnImpl<C>(
					columnName, 
					column.getColumn().getValue());
		}
		else if (column.isSetCounter_column()) {
			return new ThriftCounterColumnImpl<C>(
					columnName, 
					column.getCounter_column().getValue());
		}
		throw new UnsupportedOperationException(
				"SuperColumn " + columnName + " has no value");
	}

	@Override
	public Column<C> getColumnByIndex(int idx) {
		ColumnOrSuperColumn column = columns.get(idx);
		if (column == null) {
			// TODO: throw an exception
			return null;
		}
		else if (column.isSetColumn()) {
			return new ThriftColumnImpl<C>(
					this.colSer.fromBytes(column.getColumn().getName()), 
					column.getColumn().getValue());
		}
		else if (column.isSetCounter_column()) {
			return new ThriftCounterColumnImpl<C>(
					this.colSer.fromBytes(column.getCounter_column().getName()), 
					column.getCounter_column().getValue());
		}
		throw new UnsupportedOperationException(
				"SuperColumn " + idx + " has no value");
	}

	@Override
	public <C2> Column<C2> getSuperColumn(C columnName, Serializer<C2> colSer) {
		ColumnOrSuperColumn column = getColumn(columnName);
		if (column == null) {
			// TODO: throw an exception
			return null;
		}
		else if (column.isSetSuper_column()) {
			SuperColumn sc = column.getSuper_column();
			return new ThriftSuperColumnImpl<C2>(
					colSer.fromBytes(sc.getName()), 
					sc.getColumns());
		}
		else if (column.isSetCounter_super_column()) {
			CounterSuperColumn sc = column.getCounter_super_column();
			return new ThriftCounterSuperColumnImpl<C2>(
					colSer.fromBytes(sc.getName()), 
					sc.getColumns());
		}
		throw new UnsupportedOperationException(
				"\'" + columnName + "\' is not a composite column");
	}

	@Override
	public <C2> Column<C2> getSuperColumn(int idx, Serializer<C2> colSer) {
		ColumnOrSuperColumn column = this.columns.get(idx);
		if (column == null) {
			// TODO: throw an exception
			return null;
		}
		else if (column.isSetSuper_column()) {
			SuperColumn sc = column.getSuper_column();
			return new ThriftSuperColumnImpl<C2>(
					colSer.fromBytes(sc.getName()), 
					sc.getColumns());
		}
		else if (column.isSetCounter_super_column()) {
			CounterSuperColumn sc = column.getCounter_super_column();
			return new ThriftCounterSuperColumnImpl<C2>(
					colSer.fromBytes(sc.getName()), 
					sc.getColumns());
		}
		throw new UnsupportedOperationException(
				"\'" + idx + "\' is not a super column");
	}
	
	@Override
	public boolean isEmpty() {
		return columns.isEmpty();
	}

	@Override
	public int size() {
		return columns.size();
	}

	@Override
	public boolean isSuperColumn() {
		if (columns.isEmpty())
			return false;
		
		ColumnOrSuperColumn sosc = columns.get(0);
		return sosc.isSetSuper_column() || sosc.isSetCounter_super_column();
	}

	private ColumnOrSuperColumn getColumn(C columnName) throws InvalidOperationException {
		if (lookup == null) {
			lookup = new HashMap<C, ColumnOrSuperColumn>();
			for (ColumnOrSuperColumn column : columns) {
				if (column.isSetSuper_column()) {
					lookup.put(colSer.fromBytes(column.getSuper_column().getName()), 
							   column);
				} else if (column.isSetColumn()) {
					lookup.put(colSer.fromBytes(column.getColumn().getName()), 
							   column);
				} else if (column.isSetCounter_column()) {
					lookup.put(colSer.fromBytes(column.getCounter_column().getName()), 
							   column);
				} else if (column.isSetCounter_super_column()) {
					lookup.put(colSer.fromBytes(column.getCounter_super_column().getName()), 
							   column);
				} else {
					throw new UnsupportedOperationException(
							"Unknown column type for \'" + columnName + "\'");
				}
			}
		}
		return lookup.get(columnName);
	}
}
