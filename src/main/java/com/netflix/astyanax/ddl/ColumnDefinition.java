package com.netflix.astyanax.ddl;

public interface ColumnDefinition {
	ColumnDefinition setName(String name);
	ColumnDefinition setValidationClass(String value);
	ColumnFamilyDefinition endColumnDefinition();
}
