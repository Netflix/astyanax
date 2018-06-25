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

import com.netflix.astyanax.shaded.org.apache.cassandra.db.marshal.DoubleType;

/**
 * Uses LongSerializer via translating Doubles to and from raw long bytes form.
 * 
 * @author Yuri Finkelstein
 */
public class DoubleSerializer extends AbstractSerializer<Double> {

    private static final DoubleSerializer instance = new DoubleSerializer();

    public static DoubleSerializer get() {
        return instance;
    }

    @Override
    public ByteBuffer toByteBuffer(Double obj) {
        return LongSerializer.get().toByteBuffer(
                Double.doubleToRawLongBits(obj));
    }

    @Override
    public Double fromByteBuffer(ByteBuffer bytes) {
        if (bytes == null)
            return null;
        ByteBuffer dup = bytes.duplicate();
        return Double
                .longBitsToDouble(LongSerializer.get().fromByteBuffer(dup));
    }

    @Override
    public ByteBuffer fromString(String str) {
        return DoubleType.instance.fromString(str);
    }

    @Override
    public String getString(ByteBuffer byteBuffer) {
        return DoubleType.instance.getString(byteBuffer);
    }

    @Override
    public ByteBuffer getNext(ByteBuffer byteBuffer) {
        double val = fromByteBuffer(byteBuffer.duplicate());
        if (val == Double.MAX_VALUE) {
            throw new ArithmeticException("Can't paginate past max double");
        }
        return toByteBuffer(val + Double.MIN_VALUE);
    }

    @Override
    public ComparatorType getComparatorType() {
        return ComparatorType.DOUBLETYPE;
    }
}
