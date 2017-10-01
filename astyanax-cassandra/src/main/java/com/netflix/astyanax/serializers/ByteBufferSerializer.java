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
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.marshal.BytesType;

/**
 * The BytesExtractor is a simple identity function. It supports the Extractor
 * interface and implements the fromBytes and toBytes as simple identity
 * functions. However, the from and to methods both return the results of
 * {@link ByteBuffer#duplicate()}
 * 
 * 
 * @author Ran Tavory
 * @author zznate
 */
public final class ByteBufferSerializer extends AbstractSerializer<ByteBuffer> {

    private static ByteBufferSerializer instance = new ByteBufferSerializer();

    public static ByteBufferSerializer get() {
        return instance;
    }

    @Override
    public ByteBuffer fromByteBuffer(ByteBuffer bytes) {
        if (bytes == null) {
            return null;
        }
        return bytes.duplicate();
    }

    @Override
    public ByteBuffer toByteBuffer(ByteBuffer obj) {
        if (obj == null) {
            return null;
        }
        return obj.duplicate();
    }

    @Override
    public List<ByteBuffer> toBytesList(List<ByteBuffer> list) {
        return list;
    }

    @Override
    public List<ByteBuffer> fromBytesList(List<ByteBuffer> list) {
        return list;
    }

    @Override
    public <V> Map<ByteBuffer, V> toBytesMap(Map<ByteBuffer, V> map) {
        return map;
    }

    @Override
    public <V> Map<ByteBuffer, V> fromBytesMap(Map<ByteBuffer, V> map) {
        return map;
    }

    @Override
    public ByteBuffer fromString(String str) {
        return BytesType.instance.fromString(str);
    }

    @Override
    public String getString(ByteBuffer byteBuffer) {
        return BytesType.instance.getString(byteBuffer);
    }
}
