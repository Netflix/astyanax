package com.netflix.astyanax.entitystore;

import javax.persistence.PersistenceException;

/**
 * @param <T> entity type 
 * @param <K> rowKey type
 */
public interface EntityManager<T, K> {

	/**
	 * write entity to cassandra with mapped rowId and columns
	 * @param entity entity object
	 */
	public void put(T entity) throws PersistenceException;
	
	/**
	 * fetch whole row and construct entity object mapping from columns
	 * @param id row key
	 * @return entity object
	 */
	public T get(K id) throws PersistenceException;
	
	/**
	 * delete the whole row
	 * @param id row key
	 */
	public void delete(K id) throws PersistenceException;
}
