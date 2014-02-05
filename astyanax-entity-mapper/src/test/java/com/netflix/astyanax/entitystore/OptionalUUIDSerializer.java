package com.netflix.astyanax.entitystore;

import java.nio.ByteBuffer;
import java.util.UUID;

import com.google.common.base.Optional;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.UUIDSerializer;

public class OptionalUUIDSerializer extends AbstractSerializer<Optional<UUID>> {
    private static OptionalUUIDSerializer instance = new OptionalUUIDSerializer();

    @Override
    public ByteBuffer toByteBuffer(Optional<UUID> obj) {
        return UUIDSerializer.get().toByteBuffer(obj.orNull());
    }

    @Override
    public Optional<UUID> fromByteBuffer(ByteBuffer byteBuffer) {
        return Optional.of(UUIDSerializer.get().fromByteBuffer(byteBuffer));
    }

    public static OptionalUUIDSerializer get() {
        return instance;
    }
}
