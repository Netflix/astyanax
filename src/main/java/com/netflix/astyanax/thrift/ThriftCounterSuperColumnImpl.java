package com.netflix.astyanax.thrift;

import java.util.List;

import org.apache.cassandra.thrift.CounterColumn;

import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.exceptions.InvalidOperationException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;

public class ThriftCounterSuperColumnImpl<C> implements Column<C> {
	private final List<CounterColumn> columns;
	private final C name;

	public ThriftCounterSuperColumnImpl(C name, List<CounterColumn> columns) {
		this.name = name;
		this.columns = columns;
	}
	
	@Override
	public C getName() {
		return name;
	}

	@Override
	public <V> V getValue(Serializer<V> valSer) {
		throw new InvalidOperationException(
				"UnsupportedOperationException \'" + this.name+ "\' has no value");
	}

	@Override
	public String getStringValue() {
		throw new UnsupportedOperationException(
				"CounterSuperColumn \'" + this.name+ "\' has no value");
	}

	@Override
	public int getIntegerValue() {
		throw new UnsupportedOperationException(
				"CounterSuperColumn \'" + this.name+ "\' has no value");
	}

	@Override
	public long getLongValue() {
		throw new UnsupportedOperationException(
				"CounterSuperColumn \'" + this.name+ "\' has no value");
	}

	@Override
	public <C2> ColumnList<C2> getSubColumns(Serializer<C2> ser) {
		return new ThriftCounterColumnListImpl<C2>(this.columns, ser);
	}

	@Override
	public boolean isParentColumn() {
		return true;
	}

}
