package com.netflix.astyanax.thrift;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.KeyspaceDefinition;

public abstract class AbstractKeyspaceDefinitionImpl implements KeyspaceDefinition {
	protected final KsDef ks_def = new KsDef();

	@Override
	public KeyspaceDefinition setName(String name) {
		ks_def.setName(name);
		return this;
	}
	
	@Override
	public KeyspaceDefinition setStrategyClass(String strategyClass) {
		ks_def.setStrategy_class(strategyClass);
		return this;
	}
	
	@Override
	public KeyspaceDefinition setStrategyOptions(Map<String, String> options) {
		ks_def.setStrategy_options(options);
		return this;
	}
	
	@Override
	public ColumnFamilyDefinition beginColumnFamily() {
		if (ks_def.getCf_defs() == null) {
			ks_def.setCf_defs(new ArrayList<CfDef>());
		}
		
		final CfDef cfDef = new CfDef();
		cfDef.setColumn_type("Standard");
		cfDef.setKeyspace(ks_def.getName());
		ks_def.getCf_defs().add(cfDef);
		
		return new AbstractColumnFamilyDefinitionImpl(cfDef, this) {
			@Override
			public OperationResult<String> execute() throws ConnectionException {
				throw new IllegalStateException();
			}	
			
			@Override
			public ColumnFamilyDefinition setName(String name) {
				throw new IllegalStateException();
			}

			@Override
			public ColumnFamilyDefinition setKeyspace(String keyspace) {
				throw new IllegalStateException();
			}

			@Override
			public Future<OperationResult<String>> executeAsync() throws ConnectionException {
				throw new IllegalStateException();
			}
		};
	}
}
