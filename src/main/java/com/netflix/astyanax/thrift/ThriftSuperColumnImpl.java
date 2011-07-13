package com.netflix.astyanax.thrift;

import java.util.List;

import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;

public class ThriftSuperColumnImpl<C> implements Column<C> {
	private final List<org.apache.cassandra.thrift.Column> columns;
	private final C name;
	
	public ThriftSuperColumnImpl(C name, List<org.apache.cassandra.thrift.Column> columns) {
		this.columns = columns;
		this.name = name;
	}
	
	@Override
	public C getName() {
		return name;
	}

	@Override
	public <V> V getValue(Serializer<V> valSer) {
		throw new UnsupportedOperationException(
				"SuperColumn \'" + this.name+ "\' has no value");
	}

	@Override
	public String getStringValue() {
		throw new UnsupportedOperationException(
				"SuperColumn \'" + this.name+ "\' has no value");
	}

	@Override
	public <C2> ColumnList<C2> getSubColumns(Serializer<C2> ser) {
		return new ThriftColumnListImpl<C2>(this.columns, ser);
	}

	@Override
	public boolean isParentColumn() {
		return true;
	}

	@Override
	public int getIntegerValue() {
		throw new UnsupportedOperationException(
				"SuperColumn \'" + this.name+ "\' has no value");
	}

	@Override
	public long getLongValue()  {
		throw new UnsupportedOperationException(
				"SuperColumn \'" + this.name+ "\' has no value");
	}
}
