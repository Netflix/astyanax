package com.netflix.astyanax.model;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.netflix.astyanax.Serializer;

public interface CompositeBuilder {

    CompositeBuilder addString(String value);

    CompositeBuilder addLong(Long value);

    CompositeBuilder addInteger(Integer value);

    CompositeBuilder addBoolean(Boolean value);

    CompositeBuilder addUUID(UUID value);

    CompositeBuilder addTimeUUID(UUID value);

    CompositeBuilder addTimeUUID(Long value, TimeUnit units);

    CompositeBuilder addBytes(byte[] bytes);

    CompositeBuilder addBytes(ByteBuffer bb);

    <T> CompositeBuilder add(T value, Serializer<T> serializer);

    CompositeBuilder greaterThanEquals();

    CompositeBuilder lessThanEquals();

    ByteBuffer build();
}
