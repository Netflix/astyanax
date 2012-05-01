package com.netflix.astyanax.impl;

import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.WriteAheadEntry;
import com.netflix.astyanax.WriteAheadLog;
import com.netflix.astyanax.connectionpool.exceptions.WalException;

public class NoOpWriteAheadLog implements WriteAheadLog {

    @Override
    public WriteAheadEntry createEntry() throws WalException {
        return new WriteAheadEntry() {
            @Override
            public void readMutation(MutationBatch mutation) throws WalException {
            }

            @Override
            public void writeMutation(MutationBatch mutation) throws WalException {
            }
        };
    }

    @Override
    public void removeEntry(WriteAheadEntry entry) {
    }

    @Override
    public WriteAheadEntry readNextEntry() {
        return null;
    }

    @Override
    public void retryEntry(WriteAheadEntry entry) {
    }

}
