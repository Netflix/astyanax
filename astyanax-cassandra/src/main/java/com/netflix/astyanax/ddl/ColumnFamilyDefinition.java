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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Wrapper for a column family definition.  This provides additional utility methods on 
 * top of the existing thrift structure.  
 * 
 * @author elandau
 *
 */
public interface ColumnFamilyDefinition {

    public ColumnFamilyDefinition setComment(String comment);

    public String getComment();

    public ColumnFamilyDefinition setKeyspace(String keyspace);

    public String getKeyspace();

    @Deprecated
    public ColumnFamilyDefinition setMemtableFlushAfterMins(Integer value);

    @Deprecated
    public Integer getMemtableFlushAfterMins();

    @Deprecated
    public ColumnFamilyDefinition setMemtableOperationsInMillions(Double value);

    @Deprecated
    public Double getMemtableOperationsInMillions();

    @Deprecated
    public ColumnFamilyDefinition setMemtableThroughputInMb(Integer value);

    @Deprecated
    public Integer getMemtableThroughputInMb();

    public ColumnFamilyDefinition setMergeShardsChance(Double value);

    public Double getMergeShardsChance();

    public ColumnFamilyDefinition setMinCompactionThreshold(Integer value);

    public Integer getMinCompactionThreshold();

    public ColumnFamilyDefinition setMaxCompactionThreshold(Integer value);

    public Integer getMaxCompactionThreshold();

    public ColumnFamilyDefinition setCompactionStrategy(String strategy);

    public String getCompactionStrategy();

    public ColumnFamilyDefinition setCompactionStrategyOptions(Map<String, String>  options);
    
    public Map<String, String> getCompactionStrategyOptions();

    public ColumnFamilyDefinition setCompressionOptions(Map<String, String>  options);
    
    public Map<String, String> getCompressionOptions();
    
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

    Collection<String> getFieldNames();

    Object getFieldValue(String name);

    ColumnFamilyDefinition setFieldValue(String name, Object value);
    
    ColumnFamilyDefinition setGcGraceSeconds(Integer seconds);
    
    Integer getGcGraceSeconds();
    
    /**
     * Get metadata for all fields
     * @return
     */
    Collection<FieldMetadata> getFieldsMetadata();
    
    public void setFields(Map<String, Object> options);

    /**
     * Get the entire column family definition as a Properties object
     * with maps and lists flattened into '.' delimited properties
     * @return
     */
    Properties getProperties() throws Exception;
    
    /**
     * Set the column family definition from a properties file
     * @param properties
     * @throws Exception 
     */
    void setProperties(Properties properties) throws Exception;
}
