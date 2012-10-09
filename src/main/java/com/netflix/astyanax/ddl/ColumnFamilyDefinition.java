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
import java.util.Map;

public interface ColumnFamilyDefinition {

    ColumnFamilyDefinition setComment(String comment);

    String getComment();

    ColumnFamilyDefinition setKeyspace(String keyspace);

    String getKeyspace();

    @Deprecated
    ColumnFamilyDefinition setMemtableFlushAfterMins(Integer value);

    @Deprecated
    Integer getMemtableFlushAfterMins();

    @Deprecated
    ColumnFamilyDefinition setMemtableOperationsInMillions(Double value);

    @Deprecated
    Double getMemtableOperationsInMillions();

    @Deprecated
    ColumnFamilyDefinition setMemtableThroughputInMb(Integer value);

    @Deprecated
    Integer getMemtableThroughputInMb();

    ColumnFamilyDefinition setMergeShardsChance(Double value);

    Double getMergeShardsChance();

    ColumnFamilyDefinition setMinCompactionThreshold(Integer value);

    Integer getMinCompactionThreshold();

    ColumnFamilyDefinition setMaxCompactionThreshold(Integer value);

    Integer getMaxCompactionThreshold();

    ColumnFamilyDefinition setCompactionStrategy(String strategy);

    String getCompactionStrategy();

    ColumnFamilyDefinition setCompactionStrategyOptions(Map<String, String>  options);
    
    Map<String, String> getCompactionStrategyOptions();

    ColumnFamilyDefinition setCompressionOptions(Map<String, String>  options);
    
    Map<String, String> getCompressionOptions();
    
    ColumnFamilyDefinition setBloomFilterFpChance(Double chance);
    
    Double getBloomFilterFpChance();
    
    ColumnFamilyDefinition setCaching(String caching);
    
    String getCaching();
    
    ColumnFamilyDefinition setName(String name);

    String getName();

    ColumnFamilyDefinition setReadRepairChance(Double value);

    Double getReadRepairChance();

    ColumnFamilyDefinition setLocalReadRepairChance(Double value);

    Double getLocalReadRepairChance();

    ColumnFamilyDefinition setReplicateOnWrite(Boolean value);

    Boolean getReplicateOnWrite();

    ColumnFamilyDefinition setRowCacheProvider(String value);

    String getRowCacheProvider();

    ColumnFamilyDefinition setRowCacheSavePeriodInSeconds(Integer value);

    Integer getRowCacheSavePeriodInSeconds();

    ColumnFamilyDefinition setRowCacheSize(Double size);

    Double getRowCacheSize();

    ColumnFamilyDefinition setComparatorType(String value);

    String getComparatorType();

    ColumnFamilyDefinition setDefaultValidationClass(String value);

    String getDefaultValidationClass();

    ColumnFamilyDefinition setId(Integer id);

    Integer getId();

    ColumnFamilyDefinition setKeyAlias(ByteBuffer alias);

    ByteBuffer getKeyAlias();

    ColumnFamilyDefinition setKeyCacheSavePeriodInSeconds(Integer value);

    Integer getKeyCacheSavePeriodInSeconds();

    ColumnFamilyDefinition setKeyCacheSize(Double keyCacheSize);

    Double getKeyCacheSize();

    ColumnFamilyDefinition setKeyValidationClass(String keyValidationClass);

    String getKeyValidationClass();

    List<ColumnDefinition> getColumnDefinitionList();

    ColumnFamilyDefinition addColumnDefinition(ColumnDefinition def);

    ColumnDefinition makeColumnDefinition();

    void clearColumnDefinitionList();

    List<String> getFieldNames();

    Object getFieldValue(String name);

    ColumnFamilyDefinition setFieldValue(String name, Object value);
    
    ColumnFamilyDefinition setGcGraceSeconds(Integer seconds);
    
    Integer getGcGraceSeconds();
    
    /**
     * Get metadata for all fields
     * @return
     */
    List<FieldMetadata> getFieldsMetadata();

}
