package com.netflix.astyanax.query;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.Lists;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.serializers.*;

public abstract class AbstractPreparedCqlQuery<K, C> implements PreparedCqlQuery<K, C> {
    private List<ByteBuffer> values = Lists.newArrayList();

    protected List<ByteBuffer> getValues() {
        return values;
    }
    
    @Override
    public <V> PreparedCqlQuery<K, C> withByteBufferValue(V value, Serializer<V> serializer) {
        return withValue(serializer.toByteBuffer(value));
    }

    @Override
    public PreparedCqlQuery<K, C> withValue(ByteBuffer value) {
        values.add(value);
        return this;
    }

    @Override
    public PreparedCqlQuery<K, C> withValues(List<ByteBuffer> values) {
        this.values.addAll(values);
        return this;
    }

    @Override
    public PreparedCqlQuery<K, C> withStringValue(String value) {
        return withByteBufferValue(value, StringSerializer.get());
    }

    @Override
    public PreparedCqlQuery<K, C> withIntegerValue(Integer value) {
        return withByteBufferValue(value, IntegerSerializer.get());
    }

    @Override
    public PreparedCqlQuery<K, C> withBooleanValue(Boolean value) {
        return withByteBufferValue(value, BooleanSerializer.get());
    }

    @Override
    public PreparedCqlQuery<K, C> withDoubleValue(Double value) {
        return withByteBufferValue(value, DoubleSerializer.get());
    }

    @Override
    public PreparedCqlQuery<K, C> withLongValue(Long value) {
        return withByteBufferValue(value, LongSerializer.get());
    }

    @Override
    public PreparedCqlQuery<K, C> withFloatValue(Float value) {
        return withByteBufferValue(value, FloatSerializer.get());
    }

    @Override
    public PreparedCqlQuery<K, C> withShortValue(Short value) {
        return withByteBufferValue(value, ShortSerializer.get());
    }

    @Override
    public PreparedCqlQuery<K, C> withUUIDValue(UUID value) {
        return withByteBufferValue(value, UUIDSerializer.get());
    }

    @Override
    public PreparedCqlQuery<K, C> withTimestamp(Date value) {
        return withByteBufferValue(value, DateSerializer.get());
    }

}
