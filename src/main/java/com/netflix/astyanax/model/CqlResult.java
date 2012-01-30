package com.netflix.astyanax.model;

/**
 * Interface for a CQL query result.  The result can either be a set of rows or a count/number.
 * @author elandau
 *
 * @param <K>
 * @param <C>
 */
public interface CqlResult<K,C> {
	Rows<K,C> getRows();
	int getNumber();
	boolean hasRows();
	boolean hasNumber();
}
