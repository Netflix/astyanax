package com.netflix.astyanax.entitystore;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.persistence.PersistenceException;

import com.google.common.base.Function;

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
	 * @return entity object. null if not exist
	 */
	public T get(K id) throws PersistenceException;
	
    /**
     * delete the whole row by id
     * @param id row key
     */
    public void delete(K id) throws PersistenceException;
    
    /**
     * remove an entire entity
     * @param id row key
     */
    public void remove(T entity) throws PersistenceException;
    
	/**
	 * @return Return all entities.  
	 * 
	 * @throws PersistenceException
	 */
	public List<T> getAll() throws PersistenceException;
	
	/**
	 * @return Get a set of entities
	 * @param ids
	 * @throws PersistenceException
	 */
	public List<T> get(Collection<K> ids) throws PersistenceException;
	
	/**
	 * Delete a set of entities by their id
	 * @param ids
	 * @throws PersistenceException
	 */
	public void delete(Collection<K> ids) throws PersistenceException;
	
    /**
     * Delete a set of entities 
     * @param ids
     * @throws PersistenceException
     */
	public void remove(Collection<T> entities) throws PersistenceException;
	
	/**
	 * Store a set of entities.
	 * @param entites
	 * @throws PersistenceException
	 */
	public void put(Collection<T> entities) throws PersistenceException;
	
	/**
	 * Visit all entities.
	 * 
	 * @param callback Callback when an entity is read.  Note that the callback 
	 *                 may be called from multiple threads.
	 * @throws PersistenceException
	 */
	public void visitAll(Function<T, Boolean> callback) throws PersistenceException;
	
	/**
	 * Execute a CQL query and return the found entites
	 * @param cql
	 * @throws PersistenceException
	 */
	public List<T> find(String cql) throws PersistenceException;
	
	/**
	 * Execute a 'native' query using a simple API that adheres to cassandra's native
	 * model of rows and columns. 
	 * @return
	 */
	public NativeQuery<T, K> createNativeQuery();
	
	/**
	 * Create the underlying storage for this entity.  This should only be called
	 * once when first creating store and not part of the normal startup sequence.
	 * @throws PersistenceException
	 */
    public void createStorage(Map<String, Object> options) throws PersistenceException;
    
    /**
     * Delete the underlying storage for this entity.  
     * @param options
     * @throws PersistenceException
     */
    public void deleteStorage() throws PersistenceException;
    
    /**
     * Truncate all data in the underlying
     * @param options
     * @throws PersistenceException
     */
    public void truncate() throws PersistenceException;
    
    /**
     * Commit the internal batch after multiple operations.  Note that an entity
     * manager implementation may autocommit after each operation.
     * @throws PersistenceException
     */
    public void commit() throws PersistenceException;
}
