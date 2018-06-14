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
import java.nio.charset.Charset;

import org.apache.cassandra.db.marshal.UTF8Type;

/**
 * A StringSerializer translates the byte[] to and from string using utf-8
 * encoding.
 * 
 * @author Ran Tavory
 * 
 */
public final class StringSerializer extends AbstractSerializer<String> {

    private static final String UTF_8 = "UTF-8";
    private static final StringSerializer instance = new StringSerializer();
    private static final Charset charset = Charset.forName(UTF_8);

    public static StringSerializer get() {
        return instance;
    }

    @Override
    public ByteBuffer toByteBuffer(String obj) {
        if (obj == null) {
            return null;
        }
        return ByteBuffer.wrap(obj.getBytes(charset));
    }

    @Override
    public String fromByteBuffer(ByteBuffer byteBuffer) {
        if (byteBuffer == null) {
            return null;
        }
        final ByteBuffer dup = byteBuffer.duplicate();
        return charset.decode(dup).toString();
    }

    @Override
    public ComparatorType getComparatorType() {
        return ComparatorType.UTF8TYPE;
    }

    @Override
    public ByteBuffer fromString(String str) {
        return UTF8Type.instance.fromString(str);
    }

    @Override
    public String getString(ByteBuffer byteBuffer) {
        return UTF8Type.instance.getString(byteBuffer);
    }
}
