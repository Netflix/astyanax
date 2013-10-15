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
package com.netflix.astyanax.model;

import java.nio.ByteBuffer;
import java.util.UUID;

import com.netflix.astyanax.Serializer;

public class CompositeParserImpl implements CompositeParser {
    private final Composite composite;
    private int position = 0;

    public CompositeParserImpl(ByteBuffer bb) {
        this.composite = Composite.fromByteBuffer(bb);
    }

    @Override
    public String readString() {

        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Long readLong() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Integer readInteger() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Boolean readBoolean() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public UUID readUUID() {
        // TODO Auto-generated method stub
        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T read(Serializer<T> serializer) {
        Object obj = this.composite.get(position, serializer);
        position++;
        return (T) obj;
    }

}
