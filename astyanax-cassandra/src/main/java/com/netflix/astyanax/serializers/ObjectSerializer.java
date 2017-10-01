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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.exceptions.SerializationException;

/**
 * The ObjectSerializer is used to turn objects into their binary
 * representations.
 * 
 * @author Bozhidar Bozhanov
 * 
 */
public class ObjectSerializer extends AbstractSerializer<Object> implements
        Serializer<Object> {

    private static final ObjectSerializer INSTANCE = new ObjectSerializer();

    @Override
    public ByteBuffer toByteBuffer(Object obj) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(obj);
            oos.close();

            return ByteBuffer.wrap(baos.toByteArray());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Object fromByteBuffer(ByteBuffer bytes) {
        if ((bytes == null) || !bytes.hasRemaining()) {
            return null;
        }
        try {
            ByteBuffer dup = bytes.duplicate();
            int l = dup.remaining();
            ByteArrayInputStream bais = new ByteArrayInputStream(dup.array(),
                    dup.arrayOffset() + dup.position(), l);
            ObjectInputStream ois = new ObjectInputStream(bais);
            Object obj = ois.readObject();
            dup.position(dup.position() + (l - ois.available()));
            ois.close();
            return obj;
        } catch (Exception ex) {
            throw new SerializationException(ex);
        }
    }

    public static ObjectSerializer get() {
        return INSTANCE;
    }
}
