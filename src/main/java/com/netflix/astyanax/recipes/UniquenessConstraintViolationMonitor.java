package com.netflix.astyanax.recipes;

public interface UniquenessConstraintViolationMonitor<K,C> {
	void onViolation(K key, C column);
}
