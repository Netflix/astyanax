package com.netflix.astyanax.recipes;

import java.util.UUID;

import com.google.common.base.Supplier;
import com.netflix.astyanax.util.TimeUUIDUtils;

public class UUIDStringSupplier implements Supplier<String> {
    private static final UUIDStringSupplier instance = new UUIDStringSupplier();

    public static UUIDStringSupplier getInstance() {
        return instance;
    }

    @Override
    public String get() {
        UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMicros();
        return uuid.toString();
    }
}
