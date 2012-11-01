package com.netflix.astyanax.connectionpool;

import com.netflix.astyanax.consistency.ReadConsistencyLevelProvider;
import com.netflix.astyanax.consistency.WriteConsistencyLevelProvider;

/**
 * @author Max Morozov
 */
public interface OperationPreprocessor {
    void handleReadConsistency(ReadConsistencyLevelProvider provider);
    void handleWriteConsistency(WriteConsistencyLevelProvider provider);
}
