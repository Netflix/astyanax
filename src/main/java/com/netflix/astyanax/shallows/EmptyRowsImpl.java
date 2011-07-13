package com.netflix.astyanax.shallows;

import java.util.Iterator;

import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

public class EmptyRowsImpl<K,C> implements Rows<K,C> {

	@SuppressWarnings("unchecked")
	@Override
	public Iterator<Row<K, C>> iterator() {
		return new EmptyIterator();
	}

	@Override
	public Row<K, C> getRow(K key) {
		return null;
	}

	@Override
	public int size() {
		return 0;
	}

	@Override
	public boolean isEmpty() {
		return true;
	}
}
