package com.netflix.astyanax.ddl;

import java.nio.ByteBuffer;

import com.netflix.astyanax.Execution;

public interface ColumnFamilyDefinition extends Execution<String> {

	public ColumnFamilyDefinition setComment(String comment);
	public ColumnFamilyDefinition setKeyspace(String keyspace);
	public ColumnFamilyDefinition setMemtableFlushAfterMins(int value);
	public ColumnFamilyDefinition setMemtableOperationsInMillions(double value);
	public ColumnFamilyDefinition setMemtableThrougputInMb(int value);
	public ColumnFamilyDefinition setMergeShardsChance(double value);
	public ColumnFamilyDefinition setMinCompactionThreshold(int value);
	public ColumnFamilyDefinition setName(String name);
	public ColumnFamilyDefinition setReadRepairChance(double value);
	public ColumnFamilyDefinition setReplicateOnWrite(boolean value);
	public ColumnFamilyDefinition setRowCacheProvider(String value);
	public ColumnFamilyDefinition setRowCacheSavePeriodInSeconds(int value);
	public ColumnFamilyDefinition setRowCacheSize(int size);
	public ColumnFamilyDefinition setComparatorType(String value);
	public ColumnFamilyDefinition setDefaultValidationClass(String value);
	public ColumnFamilyDefinition setId(int id);
	public ColumnFamilyDefinition setKeyAlias(ByteBuffer alias);
	public ColumnFamilyDefinition setKeyCacheSavePeriodInSeconds(int value);
	public ColumnFamilyDefinition setKeyCacheSize(double keyCacheSize);
	public ColumnFamilyDefinition setKeyValidationClass(String keyValidationClass);
	
	public ColumnDefinition beginColumnDefinition();
	
	public KeyspaceDefinition endColumnFamilyDefinition();
}
