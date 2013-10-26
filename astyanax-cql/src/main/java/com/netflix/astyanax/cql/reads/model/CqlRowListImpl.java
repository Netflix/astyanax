package com.netflix.astyanax.cql.reads.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.cql.schema.CqlColumnFamilyDefinitionImpl;
import com.netflix.astyanax.cql.util.CqlTypeMapping;
import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

public class CqlRowListImpl<K, C> implements Rows<K, C> {

	private final List<Row<K,C>>   rows;
	private final Map<K, Row<K,C>> lookup;

	private final ColumnFamily<K,C> cf;
	private final CqlColumnFamilyDefinitionImpl cfDef; 
	private final List<ColumnDefinition> pkCols; 
	
	public CqlRowListImpl() {
		this.rows = new ArrayList<Row<K, C>>();
		this.lookup = new HashMap<K, Row<K,C>>();
		
		this.cf = null;
		this.cfDef = null;
		this.pkCols = null;
	}
	
	public CqlRowListImpl(List<com.datastax.driver.core.Row> resultRows, ColumnFamily<K,C> cf) {
		
		this.rows = new ArrayList<Row<K, C>>();
		this.lookup = new HashMap<K, Row<K,C>>();
		
		this.cf = cf;
		this.cfDef = (CqlColumnFamilyDefinitionImpl) cf.getColumnFamilyDefinition();
		this.pkCols = cfDef.getPartitionKeyColumnDefinitionList();
		
		Serializer<?> keySerializer = cf.getKeySerializer();
		K prevKey = null; 
		List<com.datastax.driver.core.Row> tempList = new ArrayList<com.datastax.driver.core.Row>();
		
		for (com.datastax.driver.core.Row row : resultRows) {
			
			K rowKey = (K) CqlTypeMapping.getDynamicColumn(row, keySerializer, 0, cf);
			
			if (prevKey == null || prevKey.equals(rowKey)) {
				tempList.add(row);
			} else {
			
				// we found a set of contiguous rows that match with the same row key
				addToResultRows(tempList);
				tempList = new ArrayList<com.datastax.driver.core.Row>();
				tempList.add(row);
			}
			prevKey = rowKey;
		}
		// flush the final list
		if (tempList.size() > 0) {
			addToResultRows(tempList);
		}
		
		for (Row<K,C> row : rows) {
			this.lookup.put(row.getKey(),  row);
		}
	}

	private void addToResultRows(List<com.datastax.driver.core.Row> rowList) {

		if (pkCols.size() == 1 || cfDef.getValueColumnDefinitionList().size() > 1) {
			for (com.datastax.driver.core.Row row : rowList) {
				this.rows.add(new CqlRowImpl<K, C>(row, cf));
			}
		} else {
			this.rows.add(new CqlRowImpl<K, C>(rowList, cf));
		}
	}
	
	@Override
	public Iterator<Row<K, C>> iterator() {
		return rows.iterator();
	}

	@Override
	public Row<K, C> getRow(K key) {
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
}
