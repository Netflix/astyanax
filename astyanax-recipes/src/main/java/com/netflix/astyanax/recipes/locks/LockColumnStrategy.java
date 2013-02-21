package com.netflix.astyanax.recipes.locks;

import com.netflix.astyanax.model.ByteBufferRange;

/**
 * Strategy used by locking and uniqueness recipes to generate
 * and check lock columns
 * 
 * @author elandau
 *
 * @param <C>
 */
public interface LockColumnStrategy<C> {
    /**
     * Return true if this is a lock column
     * @param c
     * @return
     */
    boolean isLockColumn(C c);
    
    /**
     * Return the ByteBuffer range to use when querying all lock
     * columns in a row
     * @return
     */
    ByteBufferRange getLockColumnRange();
    
    /**
     * Generate a unique lock column
     * @return
     */
    C generateLockColumn();
}
