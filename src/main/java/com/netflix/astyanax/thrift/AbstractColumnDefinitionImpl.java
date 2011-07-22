package com.netflix.astyanax.thrift;

import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;

import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.serializers.StringSerializer;

public class AbstractColumnDefinitionImpl implements ColumnDefinition {
	private final ColumnFamilyDefinition cfDef;
	private final ColumnDef columnDef;
	
	public AbstractColumnDefinitionImpl(ColumnFamilyDefinition cfDef, ColumnDef columnDef) {
		this.cfDef = cfDef;
		this.columnDef = columnDef;
		
		this.columnDef.setIndex_type(IndexType.KEYS);
	}
	
	@Override
	public ColumnDefinition setName(String name) {
		columnDef.setName(StringSerializer.get().toBytes(name));
		return this;
	}

	@Override
	public ColumnDefinition setValidationClass(String value) {
		columnDef.setValidation_class(value);
		return this;
	}
	
	@Override
	public ColumnFamilyDefinition endColumnDefinition() {
		return this.cfDef;
	}

}
