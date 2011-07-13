package com.netflix.astyanax;

/**
 * Interface for a clock used for setting the column timestamp
 * 
 * @author elandau
 *
 */
public interface Clock {
	long getCurrentTime();
}
