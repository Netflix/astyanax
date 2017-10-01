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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

import org.apache.cassandra.db.marshal.TimeUUIDType;

import com.netflix.astyanax.util.TimeUUIDUtils;

public class TimeUUIDSerializer extends UUIDSerializer {

    private static final TimeUUIDSerializer instance = new TimeUUIDSerializer();

    public static TimeUUIDSerializer get() {
        return instance;
    }

    @Override
    public ComparatorType getComparatorType() {
        return ComparatorType.TIMEUUIDTYPE;
    }

    @Override
    public ByteBuffer fromString(String str) {
        return TimeUUIDType.instance.fromString(str);
    }

    @Override
    public String getString(ByteBuffer byteBuffer) {
        if (byteBuffer == null) return null;
        ByteBuffer dup = byteBuffer.duplicate();
        long micros = TimeUUIDUtils.getMicrosTimeFromUUID(this.fromByteBuffer(dup));
        return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ").format(new Date(micros / 1000));
    }

    @Override
    public ByteBuffer getNext(ByteBuffer byteBuffer) {
        UUID uuid = fromByteBuffer(byteBuffer.duplicate());
        return toByteBuffer(new java.util.UUID(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits() + 1));
    }

}
