package com.netflix.astyanax;

/**
 * Write ahead log entry.  
 * 
 * @author elandau
 *
 */
public interface WriteAheadEntry {
	/**
	 * Commit the entry to the WAL for retries
	 */
	void commit();
	
	/**
	 * Remove the entry from the WAL
	 */
	void remove();
}
