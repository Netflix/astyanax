package com.netflix.astyanax.ddl;

import java.util.Map;

import com.netflix.astyanax.Execution;

public interface KeyspaceDefinition extends Execution<String> {

	KeyspaceDefinition setName(String name);

	KeyspaceDefinition setStrategyClass(String strategyClass);

	KeyspaceDefinition setStrategyOptions(Map<String, String> options);

	ColumnFamilyDefinition beginColumnFamily();

}
