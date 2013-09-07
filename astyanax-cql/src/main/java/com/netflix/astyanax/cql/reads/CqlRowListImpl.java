package com.netflix.astyanax.cql.reads;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.cql.util.CqlTypeMapping;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

public class CqlRowListImpl<K, C> implements Rows<K, C> {

	private List<Row<K,C>>   rows;
	private Map<K, Row<K,C>> lookup;

	public CqlRowListImpl(List<com.datastax.driver.core.Row> resultRows, ColumnFamily<?,?> cf, boolean oldStyle) {
		
		this.rows   = new ArrayList<Row<K, C>>(resultRows.size());
		if (oldStyle) {
			Serializer<?> keySerializer = cf.getKeySerializer();
			K prevKey = null; 
			List<com.datastax.driver.core.Row> tempList = new ArrayList<com.datastax.driver.core.Row>();
			for (com.datastax.driver.core.Row row : resultRows) {
				K rowKey = (K) CqlTypeMapping.getDynamicColumn(row, keySerializer, "key");
				if (prevKey == null || prevKey.equals(rowKey)) {
					tempList.add(row);
				} else {
					
					// we found a set of contiguous rows that match with the same row key
					this.rows.add(new CqlRowImpl<K, C>(tempList, cf));
					tempList = new ArrayList<com.datastax.driver.core.Row>();
					tempList.add(row);
				}
				prevKey = rowKey;
			}
			// flush the final list
			if (tempList.size() > 0) {
				this.rows.add(new CqlRowImpl<K, C>(tempList, cf));
			}
		} else {
			for (com.datastax.driver.core.Row resultRow : resultRows) {
				this.rows.add(new CqlRowImpl<K, C>(resultRow, cf));
			}
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
