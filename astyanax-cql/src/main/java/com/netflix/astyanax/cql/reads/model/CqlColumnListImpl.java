package com.netflix.astyanax.cql.reads.model;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Row;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.cql.schema.CqlColumnFamilyDefinitionImpl;
import com.netflix.astyanax.cql.util.CqlTypeMapping;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;

/**
 * Class that implements the {@link ColumnList} interface. Note that this class handles the case where the table schema
 * could contain a clustering key or just regular columns for a flat table. 
 * 
 * In the case of a flat table, each row has a unique set of columns. In the case of a clustering key, each row is a unique column.
 * There are 2 separate constructors to this class in order to handle each of these cases. 
 * 
 * @author poberai
 *
 * @param <C>
 */
@SuppressWarnings("unchecked")
public class CqlColumnListImpl<C> implements ColumnList<C> {

	private List<Column<C>> columnList = new ArrayList<Column<C>>();
	private LinkedHashMap<C, Column<C>> map = new LinkedHashMap<C, Column<C>>();
	
	public CqlColumnListImpl() {

	}
	
	/**
	 * This constructor is meant to be called when we have a table with standard columns i.e no composites, just plain columns
	 * @param row
	 */
	public CqlColumnListImpl(Row row, ColumnFamily<?,?> cf) {
		
		ColumnDefinitions cfDefinitions = row.getColumnDefinitions();
		
		int index = 1; // skip the key column
		while (index < cfDefinitions.size()) {
			String columnName = cfDefinitions.getName(index); 
			CqlColumnImpl<C> cqlCol = new CqlColumnImpl<C>((C) columnName, row, index);
			columnList.add(cqlCol);
			map.put((C) columnName, cqlCol);
			index+=3;  // skip past the ttl and the timestamp
		}
	}


	/**
	 * This constructor is meant to be used when we are using the CQL3 table but still in the legacy thrift mode
	 * @param rows
	 */
	public CqlColumnListImpl(List<Row> rows, ColumnFamily<?, ?> cf) {
		
		CqlColumnFamilyDefinitionImpl cfDef = (CqlColumnFamilyDefinitionImpl) cf.getColumnFamilyDefinition();
		
		int columnNameIndex = cfDef.getPartitionKeyColumnDefinitionList().size();  
		
		for (Row row : rows) {
			Object columnName = CqlTypeMapping.getDynamicColumn(row, cf.getColumnSerializer(), columnNameIndex, cf);
			int valueIndex = cfDef.getPartitionKeyColumnDefinitionList().size() + cfDef.getClusteringKeyColumnDefinitionList().size();
			
			CqlColumnImpl<C> cqlCol = new CqlColumnImpl<C>((C) columnName, row, valueIndex);
			columnList.add(cqlCol);
			map.put((C) columnName, cqlCol);
		}
	}
	
	public CqlColumnListImpl(List<CqlColumnImpl<C>> newColumnList) {
		this.columnList.clear();
		for (Column<C> column : newColumnList) {
			columnList.add(column);
			map.put(column.getName(), column);
		}
	}
	
	public void trimFirstColumn() {
		if (columnList.size() == 0) {
			return;
		}
		Column<C> firstCol = this.columnList.remove(0);
		map.remove(firstCol.getName());
	}

	@Override
	public Iterator<Column<C>> iterator() {
		return columnList.iterator();
	}

	@Override
	public Collection<C> getColumnNames() {
		return map.keySet();
	}

	@Override
	public Column<C> getColumnByName(C columnName) {
		return map.get(columnName);
	}

	@Override
	public String getStringValue(C columnName, String defaultValue) {
		
		Column<C> column = map.get(columnName);
		if (column == null) {
			return defaultValue;
		} else {
			return column.getStringValue();
		}
	}

	@Override
	public String getCompressedStringValue(C columnName, String defaultValue) {
		Column<C> column = map.get(columnName);
		if (column == null) {
			return defaultValue;
		} else {
			return column.getCompressedStringValue();
		}
	}

	@Override
	public Integer getIntegerValue(C columnName, Integer defaultValue) {
		Column<C> column = map.get(columnName);
		if (column == null) {
			return defaultValue;
		} else {
			return column.getIntegerValue();
		}
	}

	@Override
	public Double getDoubleValue(C columnName, Double defaultValue) {
		Column<C> column = map.get(columnName);
		if (column == null) {
			return defaultValue;
		} else {
			return column.getDoubleValue();
		}
	}

	@Override
	public Long getLongValue(C columnName, Long defaultValue) {
		Column<C> column = map.get(columnName);
		if (column == null) {
			return defaultValue;
		} else {
			return column.getLongValue();
		}
	}

	@Override
	public byte[] getByteArrayValue(C columnName, byte[] defaultValue) {
		Column<C> column = map.get(columnName);
		if (column == null) {
			return defaultValue;
		} else {
			return column.getByteArrayValue();
		}
	}

	@Override
	public Boolean getBooleanValue(C columnName, Boolean defaultValue) {
		Column<C> column = map.get(columnName);
		if (column == null) {
			return defaultValue;
		} else {
			return column.getBooleanValue();
		}
	}

	@Override
	public ByteBuffer getByteBufferValue(C columnName, ByteBuffer defaultValue) {
		Column<C> column = map.get(columnName);
		if (column == null) {
			return defaultValue;
		} else {
			return column.getByteBufferValue();
		}
	}

	@Override
	public <T> T getValue(C columnName, Serializer<T> serializer, T defaultValue) {
		Column<C> column = map.get(columnName);
		if (column == null) {
			return defaultValue;
		} else {
			return column.getValue(serializer);
		}
	}

	@Override
	public Date getDateValue(C columnName, Date defaultValue) {
		Column<C> column = map.get(columnName);
		if (column == null) {
			return defaultValue;
		} else {
			return column.getDateValue();
		}
	}

	@Override
	public UUID getUUIDValue(C columnName, UUID defaultValue) {
		Column<C> column = map.get(columnName);
		if (column == null) {
			return defaultValue;
		} else {
			return column.getUUIDValue();
		}
	}

	@Override
	public Column<C> getColumnByIndex(int idx) {
		return columnList.get(idx);
	}

	@Override
	public <C2> Column<C2> getSuperColumn(C columnName, Serializer<C2> colSer) {
		throw new UnsupportedOperationException("Operaiton not supported");
	}

	@Override
	public <C2> Column<C2> getSuperColumn(int idx, Serializer<C2> colSer) {
		throw new UnsupportedOperationException("Operaiton not supported");
	}

	@Override
	public boolean isEmpty() {
		return columnList.size() == 0;
	}

	@Override
	public int size() {
		return columnList.size();
	}

	@Override
	public boolean isSuperColumn() {
		throw new UnsupportedOperationException("Operaiton not supported");
	}
}
