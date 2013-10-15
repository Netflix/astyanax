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
package com.netflix.astyanax.model;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.Maps;

public class OrderedColumnMap<C> implements ColumnMap<C> {

    private final LinkedHashMap<C, Column<C>> columns = Maps.newLinkedHashMap();
    
    public OrderedColumnMap() {
    }
    
    public OrderedColumnMap(Collection<Column<C>> columns) {
        addAll(columns);
    }
    
    @Override
    public OrderedColumnMap<C> add(Column<C> column) {
        columns.put(column.getName(), column);
        return this;
    }
    
    @Override
    public OrderedColumnMap<C> addAll(Collection<Column<C>> columns) {
        for (Column<C> column : columns){
            this.columns.put(column.getName(), column);
        }
        return this;
    }
    
    @Override
    public Iterator<Column<C>> iterator() {
        return this.columns.values().iterator();
    }

    @Override
    public Column<C> get(C columnName) {
        return columns.get(columnName);
    }

    @Override
    public String getString(C columnName, String defaultValue) {
        Column<C> column = columns.get(columnName);
        if (column == null)
            return defaultValue;
        return column.getStringValue();
    }

    @Override
    public Integer getInteger(C columnName, Integer defaultValue) {
        Column<C> column = columns.get(columnName);
        if (column == null)
            return defaultValue;
        return column.getIntegerValue();
    }

    @Override
    public Double getDouble(C columnName, Double defaultValue) {
        Column<C> column = columns.get(columnName);
        if (column == null)
            return defaultValue;
        return column.getDoubleValue();
    }

    @Override
    public Long getLong(C columnName, Long defaultValue) {
        Column<C> column = columns.get(columnName);
        if (column == null)
            return defaultValue;
        return column.getLongValue();
    }

    @Override
    public byte[] getByteArray(C columnName, byte[] defaultValue) {
        Column<C> column = columns.get(columnName);
        if (column == null)
            return defaultValue;
        return column.getByteArrayValue();
    }

    @Override
    public Boolean getBoolean(C columnName, Boolean defaultValue) {
        Column<C> column = columns.get(columnName);
        if (column == null)
            return defaultValue;
        return column.getBooleanValue();
    }

    @Override
    public ByteBuffer getByteBuffer(C columnName, ByteBuffer defaultValue) {
        Column<C> column = columns.get(columnName);
        if (column == null)
            return defaultValue;
        return column.getByteBufferValue();
    }

    @Override
    public Date getDate(C columnName, Date defaultValue) {
        Column<C> column = columns.get(columnName);
        if (column == null)
            return defaultValue;
        return column.getDateValue();
    }

    @Override
    public UUID getUUID(C columnName, UUID defaultValue) {
        Column<C> column = columns.get(columnName);
        if (column == null)
            return defaultValue;
        return column.getUUIDValue();
    }

    @Override
    public boolean isEmpty() {
        return columns.isEmpty();
    }

    @Override
    public int size() {
        return columns.size();
    }

    @Override
    public Map<C, Column<C>> asMap() {
        return columns;
    }
}
