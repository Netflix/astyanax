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
package com.netflix.astyanax.model;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.netflix.astyanax.Serializer;

public interface CompositeBuilder {

    CompositeBuilder addString(String value);

    CompositeBuilder addLong(Long value);

    CompositeBuilder addInteger(Integer value);

    CompositeBuilder addBoolean(Boolean value);

    CompositeBuilder addUUID(UUID value);

    CompositeBuilder addTimeUUID(UUID value);

    CompositeBuilder addTimeUUID(Long value, TimeUnit units);

    CompositeBuilder addBytes(byte[] bytes);

    CompositeBuilder addBytes(ByteBuffer bb);

    <T> CompositeBuilder add(T value, Serializer<T> serializer);

    CompositeBuilder greaterThanEquals();

    CompositeBuilder lessThanEquals();

    ByteBuffer build();
}
