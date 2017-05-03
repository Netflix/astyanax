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

import com.netflix.astyanax.Serializer;

/**
 * A serializer that dynamically delegates to a proper serializer based on the
 * value passed
 * 
 * @author Bozhidar Bozhanov
 * 
 * @param <T>
 *            type
 */
public class TypeInferringSerializer<T> extends AbstractSerializer<T> implements Serializer<T> {

    @SuppressWarnings("rawtypes")
    private static final TypeInferringSerializer instance = new TypeInferringSerializer();

    @SuppressWarnings("unchecked")
    public static <T> TypeInferringSerializer<T> get() {
        return instance;
    }

    @Override
    public ByteBuffer toByteBuffer(T obj) {
        return SerializerTypeInferer.getSerializer(obj).toByteBuffer(obj);
    }

    @Override
    public T fromByteBuffer(ByteBuffer byteBuffer) {
        throw new IllegalStateException(
                "The type inferring serializer can only be used for data going to the database, and not data coming from the database");
    }

}
