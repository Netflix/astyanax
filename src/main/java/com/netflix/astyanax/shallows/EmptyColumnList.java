package com.netflix.astyanax.shallows;

import java.util.Iterator;

import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;

public class EmptyColumnList<C> implements ColumnList<C> {

	@SuppressWarnings("unchecked")
	@Override
	public Iterator<Column<C>> iterator() {
		return new EmptyIterator();
	}

	@Override
	public Column<C> getColumnByName(C columnName) {
		return null;
	}

	@Override
	public Column<C> getColumnByIndex(int idx) {
		return null;
	}

	@Override
	public <C2> Column<C2> getSuperColumn(C columnName, Serializer<C2> colSer) {
		return null;
	}

	@Override
	public <C2> Column<C2> getSuperColumn(int idx, Serializer<C2> colSer) {
		return null;
	}

	@Override
	public boolean isEmpty() {
		return true;
	}

	@Override
	public int size() {
		return 0;
	}

	@Override
	public boolean isSuperColumn() {
		return false;
	}
}
