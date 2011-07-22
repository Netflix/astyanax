package com.netflix.astyanax.query;

import com.netflix.astyanax.Execution;
import com.netflix.astyanax.model.Rows;

/**
 * Interface for executing a CQL query.
 * 
 * @author elandau
 *
 * @param <K>
 * @param <C>
 */
public interface CqlQuery<K, C> extends Execution<Rows<K, C>> {
	/**
	 * Turns on compression for the response
	 * @return
	 */
	CqlQuery<K,C> useCompression();
}
