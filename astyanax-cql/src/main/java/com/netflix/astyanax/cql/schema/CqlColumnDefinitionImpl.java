package com.netflix.astyanax.cql.schema;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.db.marshal.UTF8Type;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.datastax.driver.core.Row;
import com.netflix.astyanax.cql.util.CqlTypeMapping;
import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.ddl.FieldMetadata;

public class CqlColumnDefinitionImpl implements ColumnDefinition {

	Map<String, String> options = new HashMap<String, String>();

	public CqlColumnDefinitionImpl() {
		
	}
	
	public CqlColumnDefinitionImpl(Row row) {
		this.setName(row.getString("column_name"));
		this.setIndex(row.getString("index_name"), row.getString("index_type"));
		this.setValidationClass(row.getString("validator"));
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
		options.put("index_name", name);
		options.put("index_type", type);
		return this;
	}

	@Override
	public ColumnDefinition setKeysIndex(String name) {
		throw new NotImplementedException();
	}

	@Override
	public ColumnDefinition setKeysIndex() {
		throw new NotImplementedException();
	}

	@Override
	public ColumnDefinition setIndexWithType(String type) {
		throw new NotImplementedException();
	}

	@Override
	public String getName() {
		return options.get("column_name");
	}

	@Override
	public ByteBuffer getRawName() {
		return UTF8Type.instance.decompose(getName());
	}

	@Override
	public String getValidationClass() {
		return options.get("validator");
	}

	@Override
	public String getIndexName() {
		return options.get("index_name");
	}

	@Override
	public String getIndexType() {
		return options.get("index_type");
	}

	@Override
	public boolean hasIndex() {
		return getIndexName() != null;
	}

	@Override
	public Map<String, String> getOptions() {
		return options;
	}

	@Override
	public String getOption(String name, String defaultValue) {
		String value = options.get(name);
		if (value == null) {
			return defaultValue;
		} else {
			return value;
		}
	}

	@Override
	public ColumnDefinition setOptions(Map<String, String> index_options) {
		this.options.putAll(index_options);
		return this;
	}

	@Override
	public String setOption(String name, String value) {
		this.options.put(name, value);
		return options.get(name);
	}

	@Override
	public Collection<String> getFieldNames() {
		throw new NotImplementedException();
	}

	@Override
	public Collection<FieldMetadata> getFieldsMetadata() {
		throw new NotImplementedException();
	}

	@Override
	public Object getFieldValue(String name) {
		throw new NotImplementedException();
	}

	@Override
	public ColumnDefinition setFieldValue(String name, Object value) {
		throw new NotImplementedException();
	}

	@Override
	public ColumnDefinition setFields(Map<String, Object> fields) {
		throw new NotImplementedException();
	}

	public String getCqlType() {
		return CqlTypeMapping.getCqlType(getValidationClass());
	}
}
