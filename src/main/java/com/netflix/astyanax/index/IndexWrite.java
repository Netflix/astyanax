package com.netflix.astyanax.index;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

public interface IndexWrite<C,V,K> {

	void insertIndex(C name,V value, K pkValue) throws ConnectionException;
	
	void updateIndex(C name,V value, V oldValue,  K pkValue) throws ConnectionException;
}
