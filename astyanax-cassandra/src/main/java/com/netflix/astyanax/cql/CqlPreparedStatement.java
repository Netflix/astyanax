/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.astyanax.cql;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import com.netflix.astyanax.Execution;
import com.netflix.astyanax.Serializer;

public interface CqlPreparedStatement extends Execution<CqlStatementResult> {
    /**
     * Specify a value of custom type for which a convenience method does not exist 
     * @param value
     * @param serializer
     * @return
     */
    <V> CqlPreparedStatement withByteBufferValue(V value, Serializer<V> serializer);

    /**
     * Set the next parameter value to this ByteBuffer
     * @param value
     * @return
     */
    CqlPreparedStatement withValue(ByteBuffer value);
    
    /**
     * Add a list of ByteBuffer values
     * @param value
     * @return
     */
    CqlPreparedStatement withValues(List<ByteBuffer> value);
    
    /**
     * Set the next parameter value to this String
     * @param value
     * @return
     */
    CqlPreparedStatement withStringValue(String value);
    
    /**
     * Set the next parameter value to this Integer
     * @param value
     * @return
     */
    CqlPreparedStatement withIntegerValue(Integer value);
    
    /**
     * Set the next parameter value to this Boolean
     * @param value
     * @return
     */
    CqlPreparedStatement withBooleanValue(Boolean value);
    
    /**
     * Set the next parameter value to this Double
     * @param value
     * @return
     */
    CqlPreparedStatement withDoubleValue(Double value);

    /**
     * Set the next parameter value to this Long
     * @param value
     * @return
     */
    CqlPreparedStatement withLongValue(Long value);

    /**
     * Set the next parameter value to this Float
     * @param value
     * @return
     */
    CqlPreparedStatement withFloatValue(Float value);

    /**
     * Set the next parameter value to this Short
     * @param value
     * @return
     */
    CqlPreparedStatement withShortValue(Short value);
    
    /**
     * Set the next parameter value to this UUID
     * @param value
     * @return
     */
    CqlPreparedStatement withUUIDValue(UUID value);

    /**
     * Set the next parameter value to this Long as a timestamp
     * @param value
     * @return
     */
    CqlPreparedStatement withTimestamp(Date value);
}
