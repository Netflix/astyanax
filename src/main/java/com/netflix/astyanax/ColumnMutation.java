/*******************************************************************************
 * Copyright 2011 Netflix
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.netflix.astyanax;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.retry.RetryPolicy;

public interface ColumnMutation {
    ColumnMutation setConsistencyLevel(ConsistencyLevel consistencyLevel);

    ColumnMutation withRetryPolicy(RetryPolicy retry);
    
    /**
     * Change the default timestamp from the clock with a user supplied timestamp.
     * 
     * @param timestamp  Timestamp in microseconds
     * @return
     */
    ColumnMutation withTimestamp(long timestamp);
    
    Execution<Void> putValue(String value, Integer ttl);

    Execution<Void> putValue(byte[] value, Integer ttl);

    Execution<Void> putValue(byte value, Integer ttl);
    
    Execution<Void> putValue(short value, Integer ttl);
    
    Execution<Void> putValue(int value, Integer ttl);

    Execution<Void> putValue(long value, Integer ttl);

    Execution<Void> putValue(boolean value, Integer ttl);

    Execution<Void> putValue(ByteBuffer value, Integer ttl);

    Execution<Void> putValue(Date value, Integer ttl);

    Execution<Void> putValue(float value, Integer ttl);
    
    Execution<Void> putValue(double value, Integer ttl);

    Execution<Void> putValue(UUID value, Integer ttl);

    <T> Execution<Void> putValue(T value, Serializer<T> serializer, Integer ttl);

    Execution<Void> putEmptyColumn(Integer ttl);

    Execution<Void> incrementCounterColumn(long amount);

    Execution<Void> deleteColumn();

    Execution<Void> deleteCounterColumn();
}
