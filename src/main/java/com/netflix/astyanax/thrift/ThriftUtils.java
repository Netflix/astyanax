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
package com.netflix.astyanax.thrift;

import java.nio.ByteBuffer;

import org.apache.cassandra.thrift.SliceRange;

import com.netflix.astyanax.Serializer;

public class ThriftUtils {
    public static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer
            .wrap(new byte[0]);
    public static final SliceRange RANGE_ALL = new SliceRange(
            EMPTY_BYTE_BUFFER, EMPTY_BYTE_BUFFER, false, Integer.MAX_VALUE);
    public static final int MUTATION_OVERHEAD = 20;

    public static <C> SliceRange createSliceRange(Serializer<C> serializer,
            C startColumn, C endColumn, boolean reversed, int limit) {
        return new SliceRange((startColumn == null) ? EMPTY_BYTE_BUFFER
                : serializer.toByteBuffer(startColumn),
                (endColumn == null) ? EMPTY_BYTE_BUFFER : serializer
                        .toByteBuffer(endColumn), reversed, limit);

    }
}
