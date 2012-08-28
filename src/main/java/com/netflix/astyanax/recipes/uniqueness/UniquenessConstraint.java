package com.netflix.astyanax.recipes.uniqueness;

import com.netflix.astyanax.MutationBatch;

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
     * Release the uniqueness lock for this row.  Only call this when you no longer
     * need the uniqueness lock
     *
     * @throws Exception
     */
    void release() throws Exception;

    /**
     * Acquire the uniqueness constraint and apply the final mutation if the
     * row if found to be unique
     * @param mutation
     * @throws NotUniqueException
     * @throws Exception
     */
    void acquireAndMutate(MutationBatch mutation) throws NotUniqueException, Exception;
}
