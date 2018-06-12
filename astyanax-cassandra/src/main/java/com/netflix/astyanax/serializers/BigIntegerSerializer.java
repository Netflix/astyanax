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

import java.math.BigInteger;
import java.nio.ByteBuffer;

import com.netflix.astyanax.shaded.org.apache.cassandra.db.marshal.IntegerType;

/**
 * Serializer implementation for BigInteger
 * 
 * @author zznate
 */
public final class BigIntegerSerializer extends AbstractSerializer<BigInteger> {

    private static final BigIntegerSerializer INSTANCE = new BigIntegerSerializer();

    public static BigIntegerSerializer get() {
        return INSTANCE;
    }

    @Override
    public BigInteger fromByteBuffer(final ByteBuffer byteBuffer) {
        if (byteBuffer == null) {
            return null;
        }
        ByteBuffer dup = byteBuffer.duplicate();
        int length = dup.remaining();
        byte[] bytes = new byte[length];
        dup.get(bytes);
        return new BigInteger(bytes);
    }

    @Override
    public ByteBuffer toByteBuffer(BigInteger obj) {
        if (obj == null) {
            return null;
        }
        return ByteBuffer.wrap(obj.toByteArray());
    }

    @Override
    public ComparatorType getComparatorType() {
        return ComparatorType.INTEGERTYPE;
    }

    @Override
    public ByteBuffer fromString(String str) {
        return IntegerType.instance.fromString(str);
    }

    @Override
    public String getString(ByteBuffer byteBuffer) {
        return IntegerType.instance.getString(byteBuffer);
    }

    @Override
    public ByteBuffer getNext(final ByteBuffer byteBuffer) {
        if (byteBuffer == null)
            return null;
        BigInteger bigint = fromByteBuffer(byteBuffer.duplicate());
        bigint = bigint.add(new BigInteger("1"));
        return toByteBuffer(bigint);
    }

}
