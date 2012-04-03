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
package com.netflix.astyanax.query;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

import com.netflix.astyanax.Serializer;

public interface IndexValueExpression<K, C> {

    IndexQuery<K, C> value(String value);

    IndexQuery<K, C> value(long value);

    IndexQuery<K, C> value(int value);

    IndexQuery<K, C> value(boolean value);

    IndexQuery<K, C> value(Date value);

    IndexQuery<K, C> value(byte[] value);

    IndexQuery<K, C> value(ByteBuffer value);

    IndexQuery<K, C> value(double value);

    IndexQuery<K, C> value(UUID value);

    <V> IndexQuery<K, C> value(V value, Serializer<V> valueSerializer);
}
