package com.netflix.astyanax;

import com.netflix.astyanax.connectionpool.exceptions.TransactionException;

/**
 * Base interface for a write ahead log.  Called when a MutationBatch is send
 * to cassandra.
 * 
 * @author elandau
 *
 */
public interface WriteAheadLog {
	/**
	 * Begin the transaction.  This call may take a distributed lock or write to a WAL.
	 * @param batch
	 * @return
	 */
	WriteAheadEntry createEntry(MutationBatch batch) throws TransactionException;
}
