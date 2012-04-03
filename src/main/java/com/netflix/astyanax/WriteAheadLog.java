package com.netflix.astyanax;

import com.netflix.astyanax.connectionpool.exceptions.WalException;

/**
 * Base interface for a write ahead log. The purpose of the WAL is to provide
 * atomicity and durability when writing to Cassandra. Before writing to
 * cassandra a record is written to the WAL which is assumed to stable storage
 * able to survive from a crash or hardware failure. After a crash the system
 * will inspect the WAL for records and replay them. If a record is successfully
 * written to cassandra it will be removed from the WAL.
 * 
 * @author elandau
 * 
 */
public interface WriteAheadLog {
    /**
     * Add an entry to WAL before it is sent to Cassandra.
     * 
     * @param batch
     * @return
     */
    WriteAheadEntry createEntry() throws WalException;

    /**
     * Remove an entry from the WAL after it was successfully written to
     * cassandra
     * 
     * @param entry
     */
    void removeEntry(WriteAheadEntry entry);

    /**
     * Read the next entry to retry from the wall. Call remove if successful or
     * retryEntry if unable to write to cassandra.
     * 
     * @return
     */
    WriteAheadEntry readNextEntry();

    /**
     * Retry an entry retrieved by calling getNextEntry();
     * 
     * @param entry
     */
    void retryEntry(WriteAheadEntry entry);
}
