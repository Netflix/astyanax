package com.netflix.astyanax;

import com.netflix.astyanax.connectionpool.exceptions.WalException;

/**
 * 
 * @author elandau
 */
public interface WriteAheadEntry {
    /**
     * Fill a MutationBatch from the data in this entry
     * 
     * @param mutation
     */
    void readMutation(MutationBatch mutation) throws WalException;

    /**
     * Write the contents of this mutation to the WAL entry. Shall be called
     * only once.
     * 
     * @param mutation
     * @throws WalException
     */
    void writeMutation(MutationBatch mutation) throws WalException;
}
