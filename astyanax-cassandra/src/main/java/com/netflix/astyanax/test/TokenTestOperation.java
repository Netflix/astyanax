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
package com.netflix.astyanax.test;

import java.math.BigInteger;
import java.nio.ByteBuffer;

import com.netflix.astyanax.serializers.BigIntegerSerializer;
import com.netflix.astyanax.serializers.LongSerializer;

public class TokenTestOperation extends TestOperation {

    private final ByteBuffer rowKey;

    public TokenTestOperation(ByteBuffer rowKey) {
        this.rowKey = rowKey;
    }
    
    public TokenTestOperation(BigInteger rowKey) {
        this.rowKey = BigIntegerSerializer.get().toByteBuffer(rowKey);
    }
    
    public TokenTestOperation(Long rowKey) {
        this.rowKey = LongSerializer.get().toByteBuffer(rowKey);
    }
    
    @Override
    public ByteBuffer getRowKey() {
        return rowKey.duplicate();
    }

}
