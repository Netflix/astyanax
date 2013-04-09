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
package com.netflix.astyanax.thrift.ddl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.CfDef._Fields;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.thrift.meta_data.FieldMetaData;

import com.google.common.collect.Maps;
import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.FieldMetadata;
import com.netflix.astyanax.thrift.ThriftTypes;
import com.netflix.astyanax.thrift.ThriftUtils;

public class ThriftColumnFamilyDefinitionImpl implements ColumnFamilyDefinition {
    private final static Map<String, FieldMetadata> fieldsMetadata = Maps.newHashMap();
    
    {
        for (Entry<_Fields, FieldMetaData> field : CfDef.metaDataMap.entrySet()) {
            fieldsMetadata.put(
                    field.getValue().fieldName, 
                    new FieldMetadata(
                        field.getKey().name(), 
                        ThriftTypes.values()[field.getValue().valueMetaData.type].name(),
                        field.getValue().valueMetaData.isContainer()));
        }
    }
    
    private CfDef cfDef;

    public ThriftColumnFamilyDefinitionImpl() {
        this.cfDef = new CfDef();
    }

    public ThriftColumnFamilyDefinitionImpl(CfDef cfDef) {
        this.cfDef = cfDef;
    }

    public CfDef getThriftColumnFamilyDefinition() {
        return cfDef;
    }

    @Override
    public ColumnFamilyDefinition setComment(String comment) {
        cfDef.setComment(comment);
        return this;
    }

    @Override
    public String getComment() {
        return cfDef.getComment();
    }

    @Override
    public ColumnFamilyDefinition setKeyspace(String keyspace) {
        cfDef.setKeyspace(keyspace);
        return this;
    }

    @Override
    public String getKeyspace() {
        return cfDef.getKeyspace();
    }

    @Override
    @Deprecated
    public ColumnFamilyDefinition setMemtableFlushAfterMins(Integer value) {
        throw new RuntimeException("API Remove in Cassandra 1.0");
    }

    @Override
    @Deprecated
    public Integer getMemtableFlushAfterMins() {
        throw new RuntimeException("API Remove in Cassandra 1.0");
    }

    @Override
    @Deprecated
    public ColumnFamilyDefinition setMemtableOperationsInMillions(Double value) {
        throw new RuntimeException("API Remove in Cassandra 1.0");
    }

    @Override
    @Deprecated
    public Double getMemtableOperationsInMillions() {
        throw new RuntimeException("API Remove in Cassandra 1.0");
    }

    @Override
    @Deprecated
    public ColumnFamilyDefinition setMemtableThroughputInMb(Integer value) {
        throw new RuntimeException("API Remove in Cassandra 1.0");
    }

    @Override
    public ColumnFamilyDefinition setMergeShardsChance(Double value) {
        cfDef.setMerge_shards_chance(value);
        return this;
    }

    @Override
    public ColumnFamilyDefinition setMinCompactionThreshold(Integer value) {
        if (value != null)
            cfDef.setMin_compaction_threshold(value);
        return this;
    }

    @Override
    public ColumnFamilyDefinition setName(String name) {
        cfDef.setName(name);
        return this;
    }

    public String getName() {
        return cfDef.getName();
    }

    @Override
    public ColumnFamilyDefinition setReadRepairChance(Double value) {
        if (value != null)
            cfDef.setRead_repair_chance(value);
        return this;
    }

    @Override
    public ColumnFamilyDefinition setReplicateOnWrite(Boolean value) {
        if (value != null)
            cfDef.setReplicate_on_write(value);
        return this;
    }

    @Override
    public ColumnFamilyDefinition setRowCacheProvider(String value) {
        cfDef.setRow_cache_provider(value);
        return this;
    }

    @Override
    public ColumnFamilyDefinition setRowCacheSavePeriodInSeconds(Integer value) {
        if (value != null)
            cfDef.setRow_cache_save_period_in_seconds(value);
        return this;
    }

    @Override
    public ColumnFamilyDefinition setRowCacheSize(Double size) {
        if (size != null)
            cfDef.setRow_cache_size(size);
        return this;
    }

    @Override
    public ColumnFamilyDefinition setComparatorType(String value) {
        cfDef.setComparator_type(value);
        return this;
    }

    @Override
    public String getComparatorType() {
        return cfDef.getComparator_type();
    }

    @Override
    public ColumnFamilyDefinition setDefaultValidationClass(String value) {
        cfDef.setDefault_validation_class(value);
        return this;
    }

    @Override
    public String getDefaultValidationClass() {
        return cfDef.getDefault_validation_class();
    }

    @Override
    public ColumnFamilyDefinition setId(Integer id) {
        cfDef.setId(id);
        return this;
    }

    @Override
    public Integer getId() {
        return cfDef.getId();
    }

    @Override
    public ColumnFamilyDefinition setKeyAlias(ByteBuffer alias) {
        cfDef.setKey_alias(alias);
        return this;
    }

    @Override
    public ByteBuffer getKeyAlias() {
        if (cfDef.getKey_alias() == null)
            return null;
        return ByteBuffer.wrap(cfDef.getKey_alias());
    }

    @Override
    public ColumnFamilyDefinition setKeyCacheSavePeriodInSeconds(Integer value) {
        if (value != null)
            cfDef.setKey_cache_save_period_in_seconds(value);
        return this;
    }

    @Override
    public Integer getKeyCacheSavePeriodInSeconds() {
        return cfDef.getKey_cache_save_period_in_seconds();
    }

    @Override
    public ColumnFamilyDefinition setKeyCacheSize(Double keyCacheSize) {
        if (keyCacheSize != null)
            cfDef.setKey_cache_size(keyCacheSize);
        return this;
    }

    @Override
    public ColumnFamilyDefinition setKeyValidationClass(String keyValidationClass) {
        cfDef.setKey_validation_class(keyValidationClass);
        return this;
    }

    @Override
    public String getKeyValidationClass() {
        return cfDef.getKey_validation_class();
    }

    @Override
    public ColumnFamilyDefinition addColumnDefinition(ColumnDefinition columnDef) {
        List<ColumnDef> columns = cfDef.getColumn_metadata();
        if (columns == null) {
            columns = new ArrayList<ColumnDef>();
            cfDef.setColumn_metadata(columns);
        }

        columns.add(((ThriftColumnDefinitionImpl) columnDef).getThriftColumnDefinition());
        return this;
    }

    @Override
    public List<ColumnDefinition> getColumnDefinitionList() {
        List<ColumnDefinition> list = new ArrayList<ColumnDefinition>();

        List<ColumnDef> cdefs = cfDef.getColumn_metadata();
        if (cdefs != null) {
            for (ColumnDef cdef : cdefs) {
                list.add(new ThriftColumnDefinitionImpl(cdef));
            }
        }
        return list;
    }
    
    @Override
    public void clearColumnDefinitionList() {
        cfDef.setColumn_metadata(new ArrayList<ColumnDef>());
    }

    @Override
    @Deprecated
    public Integer getMemtableThroughputInMb() {
        throw new RuntimeException("API Remove in Cassandra 1.0");
    }

    @Override
    public Double getMergeShardsChance() {
        return cfDef.merge_shards_chance;
    }

    @Override
    public Integer getMinCompactionThreshold() {
        return cfDef.min_compaction_threshold;
    }

    @Override
    public Double getReadRepairChance() {
        return cfDef.read_repair_chance;
    }

    @Override
    public Boolean getReplicateOnWrite() {
        return cfDef.replicate_on_write;
    }

    @Override
    public String getRowCacheProvider() {
        return cfDef.row_cache_provider;
    }

    @Override
    public Integer getRowCacheSavePeriodInSeconds() {
        return cfDef.row_cache_save_period_in_seconds;
    }

    @Override
    public Double getRowCacheSize() {
        return cfDef.row_cache_size;
    }

    @Override
    public Double getKeyCacheSize() {
        return cfDef.key_cache_size;
    }

    @Override
    public Collection<String> getFieldNames() {
        return fieldsMetadata.keySet();
    }
    
    @Override
    public Object getFieldValue(String name) {
        return cfDef.getFieldValue(_Fields.valueOf(name));
    }
    
    @Override
    public ColumnFamilyDefinition setFieldValue(String name, Object value) {
        cfDef.setFieldValue(_Fields.valueOf(name), value);
        return this;
    }
    
    @Override
    public ColumnDefinition makeColumnDefinition() {
        return new ThriftColumnDefinitionImpl();
    }

    @Override
    public Collection<FieldMetadata> getFieldsMetadata() {
        return fieldsMetadata.values();
    }

    @Override
    public ColumnFamilyDefinition setMaxCompactionThreshold(Integer value) {
        if (value != null)
            cfDef.setMax_compaction_threshold(value);
        return this;
    }

    @Override
    public Integer getMaxCompactionThreshold() {
        return cfDef.getMax_compaction_threshold();
    }

    @Override
    public ColumnFamilyDefinition setCompactionStrategy(String strategy) {
        cfDef.setCompaction_strategy(strategy);
        return this;
    }

    @Override
    public String getCompactionStrategy() {
        return cfDef.getCompaction_strategy();
    }

    @Override
    public ColumnFamilyDefinition setCompactionStrategyOptions(Map<String, String> options) {
        cfDef.setCompaction_strategy_options(options);
        return this;
    }

    @Override
    public Map<String, String> getCompactionStrategyOptions() {
        return cfDef.getCompaction_strategy_options();
    }

    @Override
    public ColumnFamilyDefinition setCompressionOptions(Map<String, String> options) {
        cfDef.setCompression_options(options);
        return this;
    }

    @Override
    public Map<String, String> getCompressionOptions() {
        return cfDef.getCompression_options();
    }

    @Override
    public ColumnFamilyDefinition setBloomFilterFpChance(Double chance) {
        if (chance != null)
            cfDef.setBloom_filter_fp_chance(chance);
        return this;
    }

    @Override
    public Double getBloomFilterFpChance() {
        return cfDef.getBloom_filter_fp_chance();
    }

    @Override
    public ColumnFamilyDefinition setCaching(String caching) {
        cfDef.setCaching(caching);
        return this;
    }

    @Override
    public String getCaching() {
        return cfDef.getCaching();
    }

    @Override
    public ColumnFamilyDefinition setLocalReadRepairChance(Double value) {
        if (value != null)
            cfDef.setDclocal_read_repair_chance(value);
        return this;
    }

    @Override
    public Double getLocalReadRepairChance() {
        return cfDef.getDclocal_read_repair_chance();
    }

    @Override
    public ColumnFamilyDefinition setGcGraceSeconds(Integer seconds) {
        if (seconds != null)
            cfDef.setGc_grace_seconds(seconds);
        return this;
    }

    @Override
    public Integer getGcGraceSeconds() {
        return cfDef.getGc_grace_seconds();
    }

    @Override
    public void setFields(Map<String, Object> options) {
        for (Entry<String, FieldMetadata> field : fieldsMetadata.entrySet()) {
            String fieldName = field.getKey();
            if (options.containsKey(fieldName)) {
                if (fieldName.equals("column_metadata")) {
                    Map<String, Object> columns = (Map<String, Object>) options.get("column_metadata");
                    for (Entry<String, Object> column : columns.entrySet()) {
                        ThriftColumnDefinitionImpl columnDef = new ThriftColumnDefinitionImpl();
                        columnDef.setName(column.getKey().toString());
                        columnDef.setFields((Map<String, Object>) column.getValue());
                        
                        this.addColumnDefinition(columnDef);
                    }
                }
                else {
                    setFieldValue(field.getValue().getName(), options.get(fieldName));
                }
            }
        }
    }

    @Override
    public Properties getProperties() throws Exception {
        return ThriftUtils.getPropertiesFromThrift(cfDef);
    }

    @Override
    public void setProperties(Properties properties) throws Exception {
        ThriftUtils.populateObjectFromProperties(cfDef, properties);
    }
}
