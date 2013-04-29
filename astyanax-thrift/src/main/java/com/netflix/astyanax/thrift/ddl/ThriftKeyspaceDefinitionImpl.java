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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.KsDef._Fields;
import org.apache.thrift.meta_data.FieldMetaData;

import com.google.common.collect.Maps;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.FieldMetadata;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.thrift.ThriftTypes;
import com.netflix.astyanax.thrift.ThriftUtils;

public class ThriftKeyspaceDefinitionImpl implements KeyspaceDefinition {
    private final static Map<String, FieldMetadata> fieldsMetadata = Maps.newHashMap();
    
    {
        for (Entry<_Fields, FieldMetaData> field : KsDef.metaDataMap.entrySet()) {
            fieldsMetadata.put(
                    field.getValue().fieldName,
                    new FieldMetadata(
                        field.getKey().name(), 
                        ThriftTypes.values()[field.getValue().valueMetaData.type].name(),
                        field.getValue().valueMetaData.isContainer()));
        }
    }
    
    protected KsDef ks_def;

    public ThriftKeyspaceDefinitionImpl() {
        ks_def = new KsDef();
        ks_def.setCf_defs(new ArrayList<CfDef>());
    }

    public ThriftKeyspaceDefinitionImpl(KsDef ks_def) {
        this.ks_def = ks_def;
    }

    public KsDef getThriftKeyspaceDefinition() {
        return ks_def;
    }

    @Override
    public KeyspaceDefinition setName(String name) {
        ks_def.setName(name);
        return this;
    }

    @Override
    public String getName() {
        return ks_def.getName();
    }

    @Override
    public KeyspaceDefinition setStrategyClass(String strategyClass) {
        ks_def.setStrategy_class(strategyClass);
        return this;
    }

    @Override
    public String getStrategyClass() {
        return ks_def.getStrategy_class();
    }

    @Override
    public KeyspaceDefinition setStrategyOptions(Map<String, String> options) {
        ks_def.setStrategy_options(options);
        return this;
    }

    @Override
    public Map<String, String> getStrategyOptions() {
        if (ks_def.getStrategy_options() == null) {
            ks_def.strategy_options = new HashMap<String, String>();
        }
        return ks_def.getStrategy_options();
    }

    @Override
    public KeyspaceDefinition addStrategyOption(String name, String value) {
        getStrategyOptions().put(name, value);
        return this;
    }

    @Override
    public KeyspaceDefinition addColumnFamily(ColumnFamilyDefinition cfDef) {
        if (ks_def.getCf_defs() == null) {
            ks_def.setCf_defs(new ArrayList<CfDef>());
        }

        CfDef thriftCfDef = ((ThriftColumnFamilyDefinitionImpl) cfDef).getThriftColumnFamilyDefinition();
        thriftCfDef.setColumn_type("Standard");
        thriftCfDef.setKeyspace(ks_def.getName());
        ks_def.getCf_defs().add(thriftCfDef);
        return this;
    }

    @Override
    public List<ColumnFamilyDefinition> getColumnFamilyList() {
        List<CfDef> cfdefs = ks_def.getCf_defs();
        List<ColumnFamilyDefinition> list = new ArrayList<ColumnFamilyDefinition>();
        for (CfDef cfdef : cfdefs) {
            list.add(new ThriftColumnFamilyDefinitionImpl(cfdef));
        }
        return list;
    }

    @Override
    public ColumnFamilyDefinition getColumnFamily(String columnFamily) {
        for (CfDef cfdef : ks_def.getCf_defs()) {
            if (cfdef.getName().equalsIgnoreCase(columnFamily)) {
                return new ThriftColumnFamilyDefinitionImpl(cfdef);
            }
        }
        return null;
    }
    
    @Override
    public Collection<String> getFieldNames() {
        return fieldsMetadata.keySet();
    }
    
    @Override
    public Object getFieldValue(String name) {
        return ks_def.getFieldValue(_Fields.valueOf(name));
    }
    
    @Override
    public KeyspaceDefinition setFieldValue(String name, Object value) {
        ks_def.setFieldValue(_Fields.valueOf(name), value);
        return this;
    }

    @Override
    public Collection<FieldMetadata> getFieldsMetadata() {
        return fieldsMetadata.values();
    }

    @Override
    public void setFields(Map<String, Object> options) {
        for (Entry<String, FieldMetadata> field : fieldsMetadata.entrySet()) {
            String fieldName = field.getKey();
            if (options.containsKey(fieldName)) {
                setFieldValue(field.getValue().getName(), options.get(fieldName));
            }
        }
    }

    @Override
    public Properties getProperties() throws Exception {
        return ThriftUtils.getPropertiesFromThrift(ks_def);
    }

    @Override
    public void setProperties(Properties properties) throws Exception {
        ThriftUtils.populateObjectFromProperties(ks_def, properties);
    }

}
