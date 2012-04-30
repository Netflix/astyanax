package com.netflix.astyanax.recipes.uniqueness;

public interface UniquenessConstraint {
    /**
     * Acquire the row(s) for uniqueness. Call release() when the uniqueness on
     * the row(s) is no longer needed, such as when deleting the rows.
     * 
     * @throws NotUniqueException
     * @throws Exception
     */
    void acquire() throws NotUniqueException, Exception;

    /**
     * Release the uniqueness lock for this row.
     * 
     * @throws Exception
     */
    void release() throws Exception;
}
