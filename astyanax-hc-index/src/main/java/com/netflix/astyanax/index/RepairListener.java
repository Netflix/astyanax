package com.netflix.astyanax.index;

/**
 * An event that will be spawned when a repair condition occurs 
 * 
 * @author marcus
 *
 * @param <K>
 * @param <C>
 * @param <V>
 * 
 */
public interface RepairListener <K, C, V> {

	public void onRepair(IndexMapping<C , V> mapping, K key);
	
	
}
