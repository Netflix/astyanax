package com.netflix.astyanax.connectionpool;

/**
 * Context specific to a connection.  This interface makes it possible to store
 * connection specific state such as prepared CQL statement ids.
 * @author elandau
 *
 */
public interface ConnectionContext {
    /**
     * Set metadata identified by 'key'
     * @param key
     * @param obj
     */
    public void    setMetadata(String key, Object obj);
    
    /**
     * @return Get metadata stored by calling setMetadata
     * @param key
     */
    public Object  getMetadata(String key);
    
    /**
     * @return Return true if the metadata with the specified key exists.
     * @param key
     */
    public boolean hasMetadata(String key);
}
