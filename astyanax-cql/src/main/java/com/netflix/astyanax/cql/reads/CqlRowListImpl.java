package com.netflix.astyanax.cql.reads;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

public class CqlRowListImpl<K, C> implements Rows<K, C> {

	private List<Row<K,C>>   rows;
	private Map<K, Row<K,C>> lookup;

	public CqlRowListImpl(List<com.datastax.driver.core.Row> resultRows) {
		
		this.rows   = new ArrayList<Row<K, C>>(resultRows.size());
		for (com.datastax.driver.core.Row resultRow : resultRows) {
			this.rows.add(new CqlRowImpl<K, C>(resultRow));
		}
	}

	@Override
	public Iterator<Row<K, C>> iterator() {
		return rows.iterator();
	}

	@Override
	public Row<K, C> getRow(K key) {
		lazyBuildLookup();
		return lookup.get(key);
	}

	@Override
	public int size() {
		return rows.size();
	}

	@Override
	public boolean isEmpty() {
		return rows.isEmpty();
	}

	@Override
	public Row<K, C> getRowByIndex(int index) {
		return rows.get(index);
	}

	@Override
	public Collection<K> getKeys() {
		return Lists.transform(rows, new Function<Row<K,C>, K>() {
			@Override
			public K apply(Row<K, C> row) {
				return row.getKey();
			}
		});
	}

	private void lazyBuildLookup() {
		if (lookup == null) {
			this.lookup = Maps.newHashMap();
			for (Row<K,C> row : rows) {
				this.lookup.put(row.getKey(),  row);
			}
		}
	}
}
