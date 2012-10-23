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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.ColumnDef._Fields;
import org.apache.thrift.meta_data.FieldMetaData;

import com.google.common.collect.Lists;
import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.ddl.FieldMetadata;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftTypes;

public class ThriftColumnDefinitionImpl implements ColumnDefinition {
    private final static List<FieldMetadata> fieldsMetadata = Lists.newArrayList();
    private final static List<String> fieldNames = Lists.newArrayList();
    
    private final ColumnDef columnDef;

    {
        for (Entry<_Fields, FieldMetaData> field : ColumnDef.metaDataMap.entrySet()) {
            fieldsMetadata.add(new FieldMetadata(
                        field.getKey().name(), 
                        ThriftTypes.values()[field.getValue().valueMetaData.type].name(),
                        field.getValue().valueMetaData.isContainer()));
            fieldNames.add(field.getValue().fieldName);
        }
    }
    
    public ThriftColumnDefinitionImpl() {
        this.columnDef = new ColumnDef();
    }

    public ThriftColumnDefinitionImpl(ColumnDef columnDef) {
        this.columnDef = columnDef;
    }

    ColumnDef getThriftColumnDefinition() {
        return columnDef;
    }

    @Override
    public Map<String, String> getOptions() {
        return columnDef.getIndex_options();
    }
    
    @Override
    public ColumnDefinition setOptions(Map<String, String> index_options ) {
        columnDef.setIndex_options(index_options);
        return this;
    }
    
    @Override
    public ColumnDefinition setName(String name) {
        columnDef.setName(StringSerializer.get().toBytes(name));
        return this;
    }
    
    @Override
    public ColumnDefinition setName(ByteBuffer name) {
        columnDef.setName(name.duplicate());
        return this;
    }
    
    @Override
    public ColumnDefinition setName(byte[] name) {
        columnDef.setName(name);
        return this;
    }

    @Override
    public ColumnDefinition setValidationClass(String value) {
        columnDef.setValidation_class(value);
        return this;
    }

    @Override
    public String getName() {
        return StringSerializer.get().fromByteBuffer(getRawName().duplicate());
    }

    @Override
    public String getValidationClass() {
        return columnDef.getValidation_class();
    }

    @Override
    public ByteBuffer getRawName() {
        return ByteBuffer.wrap(this.columnDef.getName());
    }

    @Override
    public String getIndexName() {
        return this.columnDef.getIndex_name();
    }

    @Override
    public String getIndexType() {
        return this.columnDef.getIndex_type() == null ? null : this.columnDef.getIndex_type().name();
    }

    @Override
    public boolean hasIndex() {
        return this.columnDef.getIndex_type() != null;
    }

    @Override
    public ColumnDefinition setIndex(String name, String type) {
        this.columnDef.setIndex_name(name);
        this.columnDef.setIndex_type(IndexType.valueOf(type));
        return this;
    }

    @Override
    public ColumnDefinition setKeysIndex(String name) {
        this.columnDef.setIndex_name(name);
        this.columnDef.setIndex_type(IndexType.KEYS);
        return this;
    }

    @Override
    public String getOption(String name, String defaultValue) {
        if (this.columnDef != null && this.columnDef.getIndex_options() != null) {
            String value = this.columnDef.getIndex_options().get(name);
            if (value != null)
                return value;
        }
        return defaultValue;
    }
    
    @Override
    public String setOption(String name, String value) {
        if (this.columnDef.getIndex_options() == null) {
            this.columnDef.setIndex_options(new HashMap<String, String>());
        }
        return this.columnDef.getIndex_options().put(name, value);
    }
    
    @Override
    public List<String> getFieldNames() {
        return fieldNames;
    }
    
    @Override
    public Object getFieldValue(String name) {
        return columnDef.getFieldValue(_Fields.valueOf(name));
    }
    
    @Override
    public ColumnDefinition setFieldValue(String name, Object value) {
        columnDef.setFieldValue(_Fields.valueOf(name), value);
        return this;
    }

    @Override
    public List<FieldMetadata> getFieldsMetadata() {
        return fieldsMetadata;
    }

}
