package com.netflix.astyanax.shallows;

import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;

public class EmptyColumn<C> implements Column<C> {

	@Override
	public C getName() {
		throw new UnsupportedOperationException();
	}

	@Override
	public <V> V getValue(Serializer<V> valSer) {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getStringValue() {
		throw new UnsupportedOperationException();
	}

	@Override
	public <C2> ColumnList<C2> getSubColumns(Serializer<C2> ser) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isParentColumn() {
		return false;
	}

	@Override
	public int getIntegerValue() {
		throw new UnsupportedOperationException();
	}

	@Override
	public long getLongValue() {
		throw new UnsupportedOperationException();
	}
}
