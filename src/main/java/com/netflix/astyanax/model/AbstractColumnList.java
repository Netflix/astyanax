package com.netflix.astyanax.model;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

public abstract class AbstractColumnList<C> implements ColumnList<C> {
    @Override
    public String getStringValue(C columnName, String defaultValue) {
        Column<C> column = getColumnByName(columnName);
        return column != null ? column.getStringValue() : defaultValue;
    }

    @Override
    public Integer getIntegerValue(C columnName, Integer defaultValue) {
        Column<C> column = getColumnByName(columnName);
        return column != null ? column.getIntegerValue() : defaultValue;
    }

    @Override
    public Double getDoubleValue(C columnName, Double defaultValue) {
        Column<C> column = getColumnByName(columnName);
        return column != null ? column.getDoubleValue() : defaultValue;
    }

    @Override
    public Long getLongValue(C columnName, Long defaultValue) {
        Column<C> column = getColumnByName(columnName);
        return column != null ? column.getLongValue() : defaultValue;
    }

    @Override
    public byte[] getByteArrayValue(C columnName, byte[] defaultValue) {
        Column<C> column = getColumnByName(columnName);
        return column != null ? column.getByteArrayValue() : defaultValue;
    }

    @Override
    public Boolean getBooleanValue(C columnName, Boolean defaultValue) {
        Column<C> column = getColumnByName(columnName);
        return column != null ? column.getBooleanValue() : defaultValue;
    }

    @Override
    public ByteBuffer getByteBufferValue(C columnName, ByteBuffer defaultValue) {
        Column<C> column = getColumnByName(columnName);
        return column != null ? column.getByteBufferValue() : defaultValue;
    }

    @Override
    public Date getDateValue(C columnName, Date defaultValue) {
        Column<C> column = getColumnByName(columnName);
        return column != null ? column.getDateValue() : defaultValue;
    }

    @Override
    public UUID getUUIDValue(C columnName, UUID defaultValue) {
        Column<C> column = getColumnByName(columnName);
        return column != null ? column.getUUIDValue() : defaultValue;
    }

}
