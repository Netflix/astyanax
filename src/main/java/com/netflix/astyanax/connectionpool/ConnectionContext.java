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
     * Get metadata stored by calling setMetadata
     * @param key
     * @return
     */
    public Object  getMetadata(String key);
    
    /**
     * Determine if metadata exists
     * @param key
     * @return
     */
    public boolean hasMetadata(String key);
}
