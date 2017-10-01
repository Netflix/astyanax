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

/**
 * Uses Char Serializer
 * 
 * @author Todd Nine
 */
public class CharSerializer extends AbstractSerializer<Character> {

    private static final CharSerializer instance = new CharSerializer();

    public static CharSerializer get() {
        return instance;
    }

    @Override
    public ByteBuffer toByteBuffer(Character obj) {
        if (obj == null)
            return null;

        ByteBuffer buffer = ByteBuffer.allocate(Character.SIZE / Byte.SIZE);

        buffer.putChar(obj);
        buffer.rewind();

        return buffer;
    }

    @Override
    public Character fromByteBuffer(ByteBuffer bytes) {
        if (bytes == null) {
            return null;
        }
        ByteBuffer dup = bytes.duplicate();
        return dup.getChar();
    }

    @Override
    public ByteBuffer fromString(String str) {
        if (str == null || str.length() == 0)
            return null;
        return toByteBuffer(str.charAt(0));
    }

    @Override
    public String getString(ByteBuffer byteBuffer) {
        return fromByteBuffer(byteBuffer).toString();
    }

    @Override
    public ByteBuffer getNext(ByteBuffer byteBuffer) {
        throw new IllegalStateException("Char columns can't be paginated");
    }
}