package com.netflix.astyanax.recipes;

@Deprecated
public interface UniquenessConstraintViolationMonitor<K, C> {
    void onViolation(K key, C column);
}
