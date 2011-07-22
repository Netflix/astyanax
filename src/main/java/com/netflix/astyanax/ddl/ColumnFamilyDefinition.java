/*******************************************************************************
 * Copyright 2011 Netflix
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
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
