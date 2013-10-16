package com.netflix.astyanax.cql.reads;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;

import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.Row;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.cql.util.CqlTypeMapping;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;

@SuppressWarnings("unchecked")
public class CqlColumnListImpl<C> implements ColumnList<C> {

	private List<Column<C>> columnList = new ArrayList<Column<C>>();
	private LinkedHashMap<C, Column<C>> map = new LinkedHashMap<C, Column<C>>();
	
	public CqlColumnListImpl() {

	}
	
	/**
	 * This constructor is meant to be called when we are using CQL3 as is, i.e no backward compatibility with thrift
	 * @param row
	 */
	public CqlColumnListImpl(Row row) {
		
		List<Definition> cfDefinitions = row.getColumnDefinitions().asList();
		
		for (int index=0; index< cfDefinitions.size(); index++) {
			Definition def = cfDefinitions.get(index); 
			CqlColumnImpl<C> cqlCol = new CqlColumnImpl<C>((C) def.getName(), row, index);
			columnList.add(cqlCol);
			map.put((C) def.getName(), cqlCol);
		}
	}

	public CqlColumnListImpl(List<Column<C>> newColumnList) {
		this.columnList.clear();
		for (Column<C> column : newColumnList) {
			columnList.add(column);
			map.put(column.getName(), column);
		}
	}

	/**
	 * This constructor is meant to be used when we are using the CQL3 table but still in the legacy thrift mode
	 * @param rows
	 */
	public CqlColumnListImpl(List<Row> rows, ColumnFamily<?, ?> cf) {
		
		for (Row row : rows) {

			List<Definition> cfDefinitions = row.getColumnDefinitions().asList();
			
			Object columnName = CqlTypeMapping.getDynamicColumn(row, cf.getColumnSerializer());
			CqlColumnImpl<C> cqlCol = new CqlColumnImpl<C>((C) columnName, row, cfDefinitions.size()-1);
			columnList.add(cqlCol);
			map.put((C) columnName, cqlCol);
		}
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
