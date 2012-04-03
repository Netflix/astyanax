package com.netflix.astyanax.model;

import java.util.UUID;

import com.netflix.astyanax.Serializer;

public interface CompositeParser {
    String readString();

    Long readLong();

    Integer readInteger();

    Boolean readBoolean();

    UUID readUUID();

    <T> T read(Serializer<T> serializer);
}
