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

/**
 * Same as IntegerSerializer but more explicitly linked with Int32Type cmparator
 * in cassandra.
 * 
 * @author elandau
 * 
 */
public class Int32Serializer extends AbstractSerializer<Integer> {

    private static final IntegerSerializer instance = new IntegerSerializer();

    public static IntegerSerializer get() {
        return instance;
    }

    @Override
    public ByteBuffer toByteBuffer(Integer obj) {
        if (obj == null) {
            return null;
        }
        ByteBuffer b = ByteBuffer.allocate(4);
        b.putInt(obj);
        b.rewind();
        return b;
    }

    @Override
    public Integer fromByteBuffer(ByteBuffer byteBuffer) {
        if ((byteBuffer == null) || (byteBuffer.remaining() != 4)) {
            return null;
        }
        ByteBuffer dup = byteBuffer.duplicate();
        int in = dup.getInt();
        return in;
    }

    @Override
    public Integer fromBytes(byte[] bytes) {
        if ((bytes == null) || (bytes.length != 4)) {
            return null;
        }
        ByteBuffer bb = ByteBuffer.allocate(4).put(bytes, 0, 4);
        bb.rewind();
        return bb.getInt();
    }

    @Override
    public ByteBuffer fromString(String str) {
        return toByteBuffer(Integer.parseInt(str));
    }

    @Override
    public String getString(ByteBuffer byteBuffer) {
        return Integer.toString(fromByteBuffer(byteBuffer));
    }

    @Override
    public ByteBuffer getNext(ByteBuffer byteBuffer) {
        Integer val = fromByteBuffer(byteBuffer.duplicate());
        if (val == Integer.MAX_VALUE) {
            throw new ArithmeticException("Can't paginate past max int");
        }
        return toByteBuffer(val + 1);
    }

    public ComparatorType getComparatorType() {
        return ComparatorType.INT32TYPE;
    }

}