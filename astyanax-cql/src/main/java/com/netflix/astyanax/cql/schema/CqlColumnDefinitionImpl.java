package com.netflix.astyanax.cql.schema;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.marshal.UTF8Type;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.netflix.astyanax.cql.util.CqlTypeMapping;
import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.ddl.FieldMetadata;

/**
 * Impl for {@link ColumnDefinition} interface that constructs the state from a {@link Row} object 
 * retrieved from the java driver {@link ResultSet}. 
 * 
 * @author poberai
 *
 */
public class CqlColumnDefinitionImpl implements ColumnDefinition, Comparable<CqlColumnDefinitionImpl> {

	Map<String, Object> options = new HashMap<String, Object>();

	private CqlColumnType colType;
	private Integer componentIndex; 
	
	public enum CqlColumnType {
		partition_key, clustering_key, regular, compact_value;
	}
	
	public CqlColumnDefinitionImpl() {
		
	}
	
	public CqlColumnDefinitionImpl(Row row) {
		this.setName(row.getString("column_name"));
		
		String validationClass = row.getString("validator");
		if (validationClass.contains("(")) {
			int start = validationClass.indexOf("(");
			int end = validationClass.indexOf(")");
			validationClass = validationClass.substring(start+1, end);
		}
		this.setValidationClass(validationClass);
		
		colType = CqlColumnType.valueOf(row.getString("type"));
		if (colType == CqlColumnType.clustering_key) {
			componentIndex = row.getInt("component_index");
		}
	}
	
	@Override
	public ColumnDefinition setName(String name) {
		options.put("column_name", name);
		return this;
	}

	@Override
	public ColumnDefinition setName(byte[] name) {
		return setName(ByteBuffer.wrap(name));
	}

	@Override
	public ColumnDefinition setName(ByteBuffer name) {
		return setName(UTF8Type.instance.compose(name));
	}

	@Override
	public ColumnDefinition setValidationClass(String value) {
		options.put("validator", value);
		return this;
	}

	@Override
	public ColumnDefinition setIndex(String name, String type) {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override
	public ColumnDefinition setKeysIndex(String name) {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override
	public ColumnDefinition setKeysIndex() {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override
	public ColumnDefinition setIndexWithType(String type) {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override
	public String getName() {
		return (String) options.get("column_name");
	}

	@Override
	public ByteBuffer getRawName() {
		return UTF8Type.instance.decompose(getName());
	}

	@Override
	public String getValidationClass() {
		return (String) options.get("validator");
	}

	@Override
	public String getIndexName() {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override
	public String getIndexType() {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override
	public boolean hasIndex() {
		return getIndexName() != null;
	}

	@Override
	public Map<String, String> getOptions() {
		Map<String, String> result = new HashMap<String, String>();
		for (String key : options.keySet()) {
			result.put(key, options.get(key).toString());
		}
		return result;
	}

	@Override
	public String getOption(String name, String defaultValue) {
		String value = (String) options.get(name);
		if (value == null) {
			return defaultValue;
		} else {
			return value;
		}
	}

	@Override
	public ColumnDefinition setOptions(Map<String, String> setOptions) {
		this.options.putAll(setOptions);
		return this;
	}

	@Override
	public String setOption(String name, String value) {
		this.options.put(name, value);
		return options.get(name).toString();
	}

	@Override
	public Collection<String> getFieldNames() {
		return options.keySet();
	}

	@Override
	public Collection<FieldMetadata> getFieldsMetadata() {
		
		List<FieldMetadata> list = new ArrayList<FieldMetadata>();
		
		for (String key : options.keySet()) {
			Object value = options.get(key);
			Class<?> clazz = value.getClass();
			
			String name = key.toUpperCase();
			String type = clazz.getSimpleName().toUpperCase();
			boolean isContainer = Collection.class.isAssignableFrom(clazz) || Map.class.isAssignableFrom(clazz);
			list.add(new FieldMetadata(name, type, isContainer));
		}
		return list;
	}

	@Override
	public Object getFieldValue(String name) {
		return options.get(name);
	}

	@Override
	public ColumnDefinition setFieldValue(String name, Object value) {
		options.put(name, String.valueOf(value));
		return this;
	}

	@Override
	public ColumnDefinition setFields(Map<String, Object> fields) {
		options.putAll(fields);
		return this;
	}

	public String getCqlType() {
		return CqlTypeMapping.getCqlTypeFromComparator(getValidationClass());
	}
	
	public CqlColumnType getColumnType() {
		return this.colType;
	}
	
	public int getComponentIndex() {
		return this.componentIndex;
	}

	@Override
	public int compareTo(CqlColumnDefinitionImpl o) {
		return this.componentIndex.compareTo(o.componentIndex);
	}

}
