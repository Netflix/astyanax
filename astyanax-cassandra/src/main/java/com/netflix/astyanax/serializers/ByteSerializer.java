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

import org.apache.cassandra.db.marshal.IntegerType;

public class ByteSerializer extends AbstractSerializer<Byte> {

    private static final ByteSerializer instance = new ByteSerializer();

    public static ByteSerializer get() {
        return instance;
    }

    @Override
    public ByteBuffer toByteBuffer(Byte obj) {
        if (obj == null) {
            return null;
        }
        ByteBuffer b = ByteBuffer.allocate(1);
        b.put(obj);
        b.rewind();
        return b;
    }

    @Override
    public Byte fromByteBuffer(ByteBuffer byteBuffer) {
        if (byteBuffer == null) {
            return null;
        }
        ByteBuffer dup = byteBuffer.duplicate();
        byte in = dup.get();
        return in;
    }

    @Override
    public ByteBuffer fromString(String str) {
        // Verify value is a short
        return toByteBuffer(Byte.parseByte(str));
    }

    @Override
    public String getString(ByteBuffer byteBuffer) {
        return IntegerType.instance.getString(byteBuffer);
    }

    @Override
    public ByteBuffer getNext(ByteBuffer byteBuffer) {
        Byte val = fromByteBuffer(byteBuffer.duplicate());
        if (val == Byte.MAX_VALUE) {
            throw new ArithmeticException("Can't paginate past max byte");
        }
        val++;
        return toByteBuffer(val);
    }

}
