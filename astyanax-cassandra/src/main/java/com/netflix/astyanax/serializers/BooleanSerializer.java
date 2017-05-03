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
package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.BooleanType;

/**
 * Converts bytes to Boolean and vice versa
 * 
 * @author Bozhidar Bozhanov
 * 
 */
public final class BooleanSerializer extends AbstractSerializer<Boolean> {

    private static final BooleanSerializer instance = new BooleanSerializer();

    public static BooleanSerializer get() {
        return instance;
    }

    @Override
    public ByteBuffer toByteBuffer(Boolean obj) {
        if (obj == null) {
            return null;
        }
        boolean bool = obj;
        byte[] b = new byte[1];
        b[0] = bool ? (byte) 1 : (byte) 0;

        return ByteBuffer.wrap(b);
    }

    @Override
    public Boolean fromByteBuffer(ByteBuffer bytes) {
        if ((bytes == null) || (bytes.remaining() < 1)) {
            return null;
        }
        ByteBuffer dup = bytes.duplicate();
        byte b = dup.get();
        return b == (byte) 1;
     }

    @Override
    public ByteBuffer getNext(ByteBuffer byteBuffer) {
        throw new IllegalStateException("Boolean columns can't be paginated");
    }

    @Override
    public ByteBuffer fromString(String str) {
        return BooleanType.instance.fromString(str);
    }

    @Override
    public String getString(ByteBuffer byteBuffer) {
        return fromByteBuffer(byteBuffer).toString();
    }

    @Override
    public ComparatorType getComparatorType() {
        return ComparatorType.BOOLEANTYPE;
    }

}
