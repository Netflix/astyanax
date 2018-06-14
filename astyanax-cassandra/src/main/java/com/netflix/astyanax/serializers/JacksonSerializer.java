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

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize;

public class JacksonSerializer<T> extends AbstractSerializer<T> {
    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.getSerializationConfig().withSerializationInclusion(JsonSerialize.Inclusion.NON_EMPTY);
        mapper.enableDefaultTyping();
    }
    
    private final Class<T> clazz;

    public JacksonSerializer(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public ByteBuffer toByteBuffer(T entity) {
        try {
            byte[] bytes = mapper.writeValueAsBytes(entity);
            return ByteBuffer.wrap(bytes);
        } catch (Exception e) {
            throw new RuntimeException("Error serializing entity ", e);
        }
    }

    @Override
    public T fromByteBuffer(ByteBuffer byteBuffer) {
        try {
            return mapper.readValue(new ByteArrayInputStream(byteBuffer.array()), clazz);
        } catch (Exception e) {
            throw new RuntimeException("Error serializing entity ", e);
        }
    }
}
