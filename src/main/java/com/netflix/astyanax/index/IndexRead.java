package com.netflix.astyanax.index;

import java.util.Collection;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

/**
 * doesn't require Mutation.
 * 
 * @author marcus
 *
 * @param <C>
 * @param <V>
 * @param <K>
 */
public interface IndexRead<C,V,K> {

	//
	//The only operation supported currently
	//
	Collection<K> eq(C name,V value) throws ConnectionException;
}
