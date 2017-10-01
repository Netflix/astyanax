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
import java.util.UUID;

import org.apache.cassandra.db.marshal.UUIDType;

/**
 * A UUIDSerializer translates the byte[] to and from UUID types.
 * 
 * @author Ed Anuff
 * 
 */
public class UUIDSerializer extends AbstractSerializer<UUID> {

    private static final UUIDSerializer instance = new UUIDSerializer();

    public static UUIDSerializer get() {
        return instance;
    }

    @Override
    public ByteBuffer toByteBuffer(UUID uuid) {
        if (uuid == null) {
            return null;
        }
        long msb = uuid.getMostSignificantBits();
        long lsb = uuid.getLeastSignificantBits();
        byte[] buffer = new byte[16];

        for (int i = 0; i < 8; i++) {
            buffer[i] = (byte) (msb >>> 8 * (7 - i));
        }
        for (int i = 8; i < 16; i++) {
            buffer[i] = (byte) (lsb >>> 8 * (7 - i));
        }

        return ByteBuffer.wrap(buffer);
    }

    @Override
    public UUID fromByteBuffer(ByteBuffer bytes) {
        if (bytes == null) {
            return null;
        }
        ByteBuffer dup = bytes.duplicate();
        return new UUID(dup.getLong(), dup.getLong());
    }

    @Override
    public ComparatorType getComparatorType() {
        return ComparatorType.UUIDTYPE;
    }

    @Override
    public ByteBuffer fromString(String str) {
        return UUIDType.instance.fromString(str);
    }

    @Override
    public String getString(ByteBuffer byteBuffer) {
        return UUIDType.instance.getString(byteBuffer);
    }

    @Override
    public ByteBuffer getNext(ByteBuffer byteBuffer) {
        throw new RuntimeException("UUIDSerializer.getNext() Not implemented.");
    }
}
