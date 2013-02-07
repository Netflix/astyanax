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
import java.util.Date;
import java.util.UUID;

import com.netflix.astyanax.Serializer;

public abstract class AbstractColumnList<C> implements ColumnList<C> {
    @Override
    public String getStringValue(C columnName, String defaultValue) {
        Column<C> column = getColumnByName(columnName);
        if (column == null || !column.hasValue())
            return defaultValue;
        return column.getStringValue();
    }

    @Override
    public Integer getIntegerValue(C columnName, Integer defaultValue) {
        Column<C> column = getColumnByName(columnName);
        if (column == null || !column.hasValue())
            return defaultValue;
        return column.getIntegerValue();
    }

    @Override
    public Double getDoubleValue(C columnName, Double defaultValue) {
        Column<C> column = getColumnByName(columnName);
        if (column == null || !column.hasValue())
            return defaultValue;
        return column.getDoubleValue();
    }

    @Override
    public Long getLongValue(C columnName, Long defaultValue) {
        Column<C> column = getColumnByName(columnName);
        if (column == null || !column.hasValue())
            return defaultValue;
        return column.getLongValue();
    }

    @Override
    public byte[] getByteArrayValue(C columnName, byte[] defaultValue) {
        Column<C> column = getColumnByName(columnName);
        if (column == null || !column.hasValue())
            return defaultValue;
        return column.getByteArrayValue();
    }

    @Override
    public Boolean getBooleanValue(C columnName, Boolean defaultValue) {
        Column<C> column = getColumnByName(columnName);
        if (column == null || !column.hasValue())
            return defaultValue;
        return column.getBooleanValue();
    }

    @Override
    public ByteBuffer getByteBufferValue(C columnName, ByteBuffer defaultValue) {
        Column<C> column = getColumnByName(columnName);
        if (column == null || !column.hasValue())
            return defaultValue;
        return column.getByteBufferValue();
    }

    @Override
    public Date getDateValue(C columnName, Date defaultValue) {
        Column<C> column = getColumnByName(columnName);
        if (column == null || !column.hasValue())
            return defaultValue;
        return column.getDateValue();
    }

    @Override
    public UUID getUUIDValue(C columnName, UUID defaultValue) {
        Column<C> column = getColumnByName(columnName);
        if (column == null || !column.hasValue())
            return defaultValue;
        return column.getUUIDValue();
    }

    @Override
    public <T> T getValue(C columnName, Serializer<T> serializer, T defaultValue) {
        Column<C> column = getColumnByName(columnName);
        if (column == null || !column.hasValue())
            return defaultValue;
        return column.getValue(serializer);
    }

    @Override
    public String getCompressedStringValue(C columnName, String defaultValue) {
        Column<C> column = getColumnByName(columnName);
        if (column == null || !column.hasValue())
            return defaultValue;
        return column.getCompressedStringValue();
    }

}
