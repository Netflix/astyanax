package com.netflix.astyanax;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

/**
 * The mutation manager enables different recipes or entity managers to use
 * the same mutation for bulk operations.  In addition a mutation manager 
 * can be used to implement write ahead semantics for 'atomic' mutations.
 * 
 * The mutation manager is expected to be thread safe such that all mutations
 * in the same thread go to the same mutation batch.  This also means that
 * getMutationBatch(), the mutations and the final commit() must occur within
 * the same thread.
 * 
 * @author elandau
 *
 */
public interface MutationBatchManager {
    /**
     * Get or create a new mutation batch.
     * 
     * @return
     */
    public MutationBatch getSharedMutationBatch();
    
    /**
     * Get a one time mutation batch
     * @return
     */
    public MutationBatch getNewMutationBatch();
    
    /**
     * Commit all mutations on the batch
     * @throws ConnectionException
     */
    public void commitSharedMutationBatch() throws ConnectionException ;
    
    /**
     * Discard all mutations on the batch
     */
    public void discard();
}
