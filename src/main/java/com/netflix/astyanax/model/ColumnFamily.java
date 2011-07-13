package com.netflix.astyanax.model;

import com.netflix.astyanax.Serializer;

/**
 * Basic column family definition.  The column family definition encapsulates
 * the column family name as well as the type and serializers for the row keys
 * and first level columns.  Super column subcolumn name type and serializers
 * are specified using a ColumnPath.
 * 
 * @author elandau
 *
 * @param <K>
 * @param <C>
 */
public class ColumnFamily<K,C> {
	private final String columnFamilyName;
	private final Serializer<K> keySerializer;
	private final Serializer<C> columnSerializer;
	private final ColumnType type;

	public ColumnFamily(String columnFamilyName,
		Serializer<K> keySerializer,
		Serializer<C> columnSerializer,
		ColumnType type) {
		this.columnFamilyName = columnFamilyName;
		this.keySerializer = keySerializer;
		this.columnSerializer = columnSerializer;
		this.type = type;
	}
	
	public ColumnFamily(String columnFamilyName,
		Serializer<K> keySerializer,
		Serializer<C> columnSerializer) {
		this.columnFamilyName = columnFamilyName;
		this.keySerializer = keySerializer;
		this.columnSerializer = columnSerializer;
		this.type = ColumnType.STANDARD;
	}
	
	
	public String getName() {
		return columnFamilyName;
	}

	/**
	 * Serializer for first level column names.  This serializer does not apply
	 * to sub column names.
	 * 
	 * @return
	 */
	public Serializer<C> getColumnSerializer() {
		return columnSerializer;
	}

	/**
	 * Serializer used to generate row keys. 
	 * @return
	 */
	public Serializer<K> getKeySerializer() {
		return keySerializer;
	}

	/**
	 * Type of columns in this column family (Standard or Super)
	 * @return
	 */
	public ColumnType getType() {
		return type;
	}
}
