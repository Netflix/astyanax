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
package com.netflix.astyanax.partitioner;

import java.nio.ByteBuffer;

import com.netflix.astyanax.serializers.BigIntegerSerializer;

public class OrderedBigIntegerPartitioner extends BigInteger127Partitioner {

    private static final OrderedBigIntegerPartitioner instance = new OrderedBigIntegerPartitioner();
    public static Partitioner get() {
        return instance;
    }

    protected OrderedBigIntegerPartitioner() {
        
    }
    
    @Override
    public String getTokenForKey(ByteBuffer key) {
        return BigIntegerSerializer.get().fromByteBuffer(key).toString();
    }
}