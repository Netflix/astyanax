package com.netflix.astyanax.model;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.serializers.BooleanSerializer;
import com.netflix.astyanax.serializers.ByteBufferSerializer;
import com.netflix.astyanax.serializers.ByteSerializer;
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.serializers.DateSerializer;
import com.netflix.astyanax.serializers.DoubleSerializer;
import com.netflix.astyanax.serializers.FloatSerializer;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.ShortSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.serializers.UUIDSerializer;

public abstract class AbstractColumnImpl <C> implements Column<C> {
    private final C name;

    public AbstractColumnImpl(C name) {
        this.name = name;
    }

    @Override
    public final C getName() {
        return name;
    }

    @Override
    public final String getStringValue() {
        return getValue(StringSerializer.get());
    }

    @Override
    public final byte getByteValue() {
        return getValue(ByteSerializer.get());
    }
    
    @Override
    public final short getShortValue() {
        return getValue(ShortSerializer.get());
    }
    
    @Override
    public final int getIntegerValue() {
        return getValue(IntegerSerializer.get());
    }

    @Override
    public long getLongValue() {
        return getValue(LongSerializer.get());
    }

    @Override
    public final byte[] getByteArrayValue() {
        return getValue(BytesArraySerializer.get());
    }

    @Override
    public final boolean getBooleanValue() {
        return getValue(BooleanSerializer.get());
    }

    @Override
    public final ByteBuffer getByteBufferValue() {
        return getValue(ByteBufferSerializer.get());
    }

    @Override
    public final Date getDateValue() {
        return getValue(DateSerializer.get());
    }

    @Override
    public final UUID getUUIDValue() {
        return getValue(UUIDSerializer.get());
    }

    @Override
    public final float getFloatValue() {
        return getValue(FloatSerializer.get());
    }
    
    @Override
    public final double getDoubleValue() {
        return getValue(DoubleSerializer.get());
    }

    @Override
    public final String getCompressedStringValue() {
        throw new UnsupportedOperationException("getCompressedString not yet implemented");
    }

    @Override
    public <C2> ColumnList<C2> getSubColumns(Serializer<C2> ser) {
        throw new UnsupportedOperationException("SimpleColumn \'" + name + "\' has no children");
    }

    @Override
    public boolean isParentColumn() {
        return false;
    }


}