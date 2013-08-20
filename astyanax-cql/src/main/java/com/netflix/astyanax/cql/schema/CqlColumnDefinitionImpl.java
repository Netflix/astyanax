package com.netflix.astyanax.cql.schema;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;

import org.apache.cassandra.db.marshal.UTF8Type;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.netflix.astyanax.cql.util.CqlTypeMapping;
import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.ddl.FieldMetadata;

public class CqlColumnDefinitionImpl implements ColumnDefinition {

	private String validationClass;
	private String name; 

	@Override
	public ColumnDefinition setName(String name) {
		this.name = name; 
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
		validationClass = value;
		return this;
	}

	@Override
	public ColumnDefinition setIndex(String name, String type) {
		throw new NotImplementedException();
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
		return name;
	}

	@Override
	public ByteBuffer getRawName() {
		return UTF8Type.instance.decompose(name);
	}

	@Override
	public String getValidationClass() {
		return validationClass;
	}

	@Override
	public String getIndexName() {
		throw new NotImplementedException();
	}

	@Override
	public String getIndexType() {
		throw new NotImplementedException();
	}

	@Override
	public boolean hasIndex() {
		throw new NotImplementedException();
	}

	@Override
	public Map<String, String> getOptions() {
		throw new NotImplementedException();
	}

	@Override
	public String getOption(String name, String defaultValue) {
		throw new NotImplementedException();
	}

	@Override
	public ColumnDefinition setOptions(Map<String, String> index_options) {
		throw new NotImplementedException();
	}

	@Override
	public String setOption(String name, String value) {
		throw new NotImplementedException();
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
