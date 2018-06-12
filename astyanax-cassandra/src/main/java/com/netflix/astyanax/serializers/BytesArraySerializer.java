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
package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;

import com.netflix.astyanax.shaded.org.apache.cassandra.db.marshal.BytesType;

import com.netflix.astyanax.Serializer;

/**
 * A BytesArraySerializer translates the byte[] to and from ByteBuffer.
 * 
 * @author Patricio Echague
 * 
 */
public final class BytesArraySerializer extends AbstractSerializer<byte[]> implements Serializer<byte[]> {

    private static final BytesArraySerializer instance = new BytesArraySerializer();

    public static BytesArraySerializer get() {
        return instance;
    }

    @Override
    public ByteBuffer toByteBuffer(byte[] obj) {
        if (obj == null) {
            return null;
        }
        return ByteBuffer.wrap(obj);
    }

    @Override
    public byte[] fromByteBuffer(ByteBuffer byteBuffer) {
            if (byteBuffer == null) {
                return null;
            }
            ByteBuffer dup = byteBuffer.duplicate();
            byte[] bytes = new byte[dup.remaining()];
            byteBuffer.get(bytes, 0, bytes.length);
            return bytes;
    }

    @Override
    public ByteBuffer fromString(String str) {
        return BytesType.instance.fromString(str);
    }

    @Override
    public String getString(ByteBuffer byteBuffer) {
	    if (byteBuffer == null) return null;
            return BytesType.instance.getString(byteBuffer.duplicate());
    }
}
