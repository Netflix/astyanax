package com.netflix.astyanax.thrift;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;

import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.KeyspaceDefinition;

public abstract class AbstractColumnFamilyDefinitionImpl implements ColumnFamilyDefinition {
	private CfDef cfDef;
	private KeyspaceDefinition ksDef;
	
	public AbstractColumnFamilyDefinitionImpl(CfDef cfDef, KeyspaceDefinition ksDef) {
		this.cfDef = cfDef;
		this.ksDef = ksDef;
	}
	
	@Override
	public ColumnFamilyDefinition setComment(String comment) {
		cfDef.setComment(comment);
		return this;
	}
	
	@Override
	public ColumnFamilyDefinition setKeyspace(String keyspace) {
		cfDef.setKeyspace(keyspace);
		return this;
	}
	
	@Override
	public ColumnFamilyDefinition setMemtableFlushAfterMins(int value) {
		cfDef.setMemtable_flush_after_mins(value);
		return this;
	}

	@Override
	public ColumnFamilyDefinition setMemtableOperationsInMillions(double value) {
		cfDef.setMemtable_operations_in_millions(value);
		return this;
	}
	
	@Override
	public ColumnFamilyDefinition setMemtableThrougputInMb(int value) {
		cfDef.setMemtable_throughput_in_mb(value);
		return this;
	}
	
	@Override
	public ColumnFamilyDefinition setMergeShardsChance(double value) {
		cfDef.setMerge_shards_chance(value);
		return this;
	}
	
	@Override
	public ColumnFamilyDefinition setMinCompactionThreshold(int value) {
		cfDef.setMin_compaction_threshold(value);
		return this;
	}
	
	@Override
	public ColumnFamilyDefinition setName(String name) {
		cfDef.setName(name);
		return this;
	}
	
	@Override
	public ColumnFamilyDefinition setReadRepairChance(double value) {
		cfDef.setRead_repair_chance(value);
		return this;
	}
	
	@Override
	public ColumnFamilyDefinition setReplicateOnWrite(boolean value) {
		cfDef.setReplicate_on_write(value);
		return this;
	}
	
	@Override
	public ColumnFamilyDefinition setRowCacheProvider(String value){
		cfDef.setRow_cache_provider(value);
		return this;
	}
	
	@Override
	public ColumnFamilyDefinition setRowCacheSavePeriodInSeconds(int value) {
		cfDef.setRow_cache_save_period_in_seconds(value);
		return this;
	}
	
	@Override
	public ColumnFamilyDefinition setRowCacheSize(int size) {
		cfDef.setRow_cache_size(size);
		return this;
	}
	
	@Override
	public ColumnFamilyDefinition setComparatorType(String value) {
		cfDef.setComparator_type(value);
		return this;
	}
	
	@Override
	public ColumnFamilyDefinition setDefaultValidationClass(String value) {
		cfDef.setDefault_validation_class(value);
		return this;
	}
	
	@Override
	public ColumnFamilyDefinition setId(int id) {
		cfDef.setId(id);
		return this;
	}
	
	@Override
	public ColumnFamilyDefinition setKeyAlias(ByteBuffer alias) {
		cfDef.setKey_alias(alias);
		return this;
	}
	
	@Override
	public ColumnFamilyDefinition setKeyCacheSavePeriodInSeconds(int value) {
		cfDef.setKey_cache_save_period_in_seconds(value);
		return this;
	}
	
	@Override
	public ColumnFamilyDefinition setKeyCacheSize(double keyCacheSize) {
		cfDef.setKey_cache_size(keyCacheSize);
		return this;
	}
	
	@Override
	public ColumnFamilyDefinition setKeyValidationClass(String keyValidationClass) {
		cfDef.setKey_validation_class(keyValidationClass);
		return this;
	}
	
	@Override
	public ColumnDefinition beginColumnDefinition() {
		List<ColumnDef> columns = cfDef.getColumn_metadata();
		if (columns == null) {
			columns = new ArrayList<ColumnDef>();
			cfDef.setColumn_metadata(columns);
		}
		
		ColumnDef columnDef = new ColumnDef();
		
		ColumnDefinition column = new AbstractColumnDefinitionImpl(this, columnDef);
		columns.add(columnDef);
		return column;
	}
	
	@Override
	public KeyspaceDefinition endColumnFamilyDefinition() {
		return ksDef;
	}
}
