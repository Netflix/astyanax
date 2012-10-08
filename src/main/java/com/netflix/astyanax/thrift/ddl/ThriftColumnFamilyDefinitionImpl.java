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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.CfDef._Fields;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.thrift.meta_data.FieldMetaData;

import com.google.common.collect.Lists;
import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.FieldMetadata;
import com.netflix.astyanax.thrift.ThriftTypes;

public class ThriftColumnFamilyDefinitionImpl implements ColumnFamilyDefinition {
    private final static List<FieldMetadata> fieldsMetadata = Lists.newArrayList();
    private final static List<String> fieldNames = Lists.newArrayList();
    
    {
        for (Entry<_Fields, FieldMetaData> field : CfDef.metaDataMap.entrySet()) {
            fieldsMetadata.add(new FieldMetadata(
                        field.getKey().name(), 
                        ThriftTypes.values()[field.getValue().valueMetaData.type].name(),
                        field.getValue().valueMetaData.isContainer()));
            fieldNames.add(field.getValue().fieldName);
        }
    }
    
    private final CfDef cfDef;

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
    public ColumnFamilyDefinition setMemtableFlushAfterMins(int value) {
        throw new RuntimeException("API Remove in Cassandra 1.0");
    }

    @Override
    @Deprecated
    public int getMemtableFlushAfterMins() {
        throw new RuntimeException("API Remove in Cassandra 1.0");
    }

    @Override
    @Deprecated
    public ColumnFamilyDefinition setMemtableOperationsInMillions(double value) {
        throw new RuntimeException("API Remove in Cassandra 1.0");
    }

    @Override
    @Deprecated
    public double getMemtableOperationsInMillions() {
        throw new RuntimeException("API Remove in Cassandra 1.0");
    }

    @Override
    @Deprecated
    public ColumnFamilyDefinition setMemtableThroughputInMb(int value) {
        throw new RuntimeException("API Remove in Cassandra 1.0");
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

    public String getName() {
        return cfDef.getName();
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
    public ColumnFamilyDefinition setRowCacheProvider(String value) {
        cfDef.setRow_cache_provider(value);
        return this;
    }

    @Override
    public ColumnFamilyDefinition setRowCacheSavePeriodInSeconds(int value) {
        cfDef.setRow_cache_save_period_in_seconds(value);
        return this;
    }

    @Override
    public ColumnFamilyDefinition setRowCacheSize(double size) {
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
    public ColumnFamilyDefinition setId(int id) {
        cfDef.setId(id);
        return this;
    }

    @Override
    public int getId() {
        return cfDef.getId();
    }

    @Override
    public ColumnFamilyDefinition setKeyAlias(ByteBuffer alias) {
        cfDef.setKey_alias(alias);
        return this;
    }

    @Override
    public ByteBuffer getKeyAlias() {
        return ByteBuffer.wrap(cfDef.getKey_alias());
    }

    @Override
    public ColumnFamilyDefinition setKeyCacheSavePeriodInSeconds(int value) {
        cfDef.setKey_cache_save_period_in_seconds(value);
        return this;
    }

    @Override
    public int getKeyCacheSavePeriodInSeconds() {
        return cfDef.getKey_cache_save_period_in_seconds();
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
    public int getMemtableThroughputInMb() {
        throw new RuntimeException("API Remove in Cassandra 1.0");
    }

    @Override
    public double getMergeShardsChance() {
        return cfDef.merge_shards_chance;
    }

    @Override
    public int getMinCompactionThreshold() {
        return cfDef.min_compaction_threshold;
    }

    @Override
    public double getReadRepairChance() {
        return cfDef.read_repair_chance;
    }

    @Override
    public boolean getReplicateOnWrite() {
        return cfDef.replicate_on_write;
    }

    @Override
    public String getRowCacheProvider() {
        return cfDef.row_cache_provider;
    }

    @Override
    public int getRowCacheSavePeriodInSeconds() {
        return cfDef.row_cache_save_period_in_seconds;
    }

    @Override
    public double getRowCacheSize() {
        return cfDef.row_cache_size;
    }

    @Override
    public double getKeyCacheSize() {
        return cfDef.key_cache_size;
    }

    @Override
    public List<String> getFieldNames() {
        return fieldNames;
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
    public List<FieldMetadata> getFieldsMetadata() {
        return fieldsMetadata;
    }

    @Override
    public ColumnFamilyDefinition setMaxCompactionThreshold(int value) {
        cfDef.setMax_compaction_threshold(value);
        return this;
    }

    @Override
    public int getMaxCompactionThreshold() {
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
    public ColumnFamilyDefinition setBloomFilterFpChance(double chance) {
        cfDef.setBloom_filter_fp_chance(chance);
        return this;
    }

    @Override
    public double getBloomFilterFpChance() {
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
    public ColumnFamilyDefinition setLocalReadRepairChance(double value) {
        cfDef.setDclocal_read_repair_chance(value);
        return this;
    }

    @Override
    public double getLocalReadRepairChance() {
        return cfDef.getDclocal_read_repair_chance();
    }

    @Override
    public ColumnFamilyDefinition setGcGraceSeconds(int seconds) {
        cfDef.setGc_grace_seconds(seconds);
        return this;
    }

    @Override
    public int getGcGraceSeconds() {
        return cfDef.getGc_grace_seconds();
    }
}
