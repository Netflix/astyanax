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
import java.util.List;

public interface ColumnFamilyDefinition {

    ColumnFamilyDefinition setComment(String comment);

    String getComment();

    ColumnFamilyDefinition setKeyspace(String keyspace);

    String getKeyspace();

    @Deprecated
    ColumnFamilyDefinition setMemtableFlushAfterMins(int value);

    @Deprecated
    int getMemtableFlushAfterMins();

    @Deprecated
    ColumnFamilyDefinition setMemtableOperationsInMillions(double value);

    @Deprecated
    double getMemtableOperationsInMillions();

    @Deprecated
    ColumnFamilyDefinition setMemtableThroughputInMb(int value);

    @Deprecated
    int getMemtableThroughputInMb();

    ColumnFamilyDefinition setMergeShardsChance(double value);

    double getMergeShardsChance();

    ColumnFamilyDefinition setMinCompactionThreshold(int value);

    int getMinCompactionThreshold();

    ColumnFamilyDefinition setName(String name);

    String getName();

    ColumnFamilyDefinition setReadRepairChance(double value);

    double getReadRepairChance();

    ColumnFamilyDefinition setReplicateOnWrite(boolean value);

    boolean getReplicateOnWrite();

    ColumnFamilyDefinition setRowCacheProvider(String value);

    String getRowCacheProvider();

    ColumnFamilyDefinition setRowCacheSavePeriodInSeconds(int value);

    int getRowCacheSavePeriodInSeconds();

    ColumnFamilyDefinition setRowCacheSize(double size);

    double getRowCacheSize();

    ColumnFamilyDefinition setComparatorType(String value);

    String getComparatorType();

    ColumnFamilyDefinition setDefaultValidationClass(String value);

    String getDefaultValidationClass();

    ColumnFamilyDefinition setId(int id);

    int getId();

    ColumnFamilyDefinition setKeyAlias(ByteBuffer alias);

    ByteBuffer getKeyAlias();

    ColumnFamilyDefinition setKeyCacheSavePeriodInSeconds(int value);

    int getKeyCacheSavePeriodInSeconds();

    ColumnFamilyDefinition setKeyCacheSize(double keyCacheSize);

    double getKeyCacheSize();

    ColumnFamilyDefinition setKeyValidationClass(String keyValidationClass);

    String getKeyValidationClass();

    List<ColumnDefinition> getColumnDefinitionList();

    ColumnFamilyDefinition addColumnDefinition(ColumnDefinition def);

    ColumnDefinition makeColumnDefinition();
}
