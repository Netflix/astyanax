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

import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;

import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.serializers.StringSerializer;

public class ThriftColumnDefinitionImpl implements ColumnDefinition {
    private final ColumnDef columnDef;

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
    public ColumnDefinition setName(String name) {
        columnDef.setName(StringSerializer.get().toBytes(name));
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
        return this.columnDef.getIndex_type() != null && this.columnDef.getIndex_type() == IndexType.KEYS;
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
}
