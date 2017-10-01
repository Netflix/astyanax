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

public interface PreparedIndexValueExpression<K, C> {

    PreparedIndexExpression<K, C> value(String value);

    PreparedIndexExpression<K, C> value(long value);

    PreparedIndexExpression<K, C> value(int value);

    PreparedIndexExpression<K, C> value(boolean value);

    PreparedIndexExpression<K, C> value(Date value);

    PreparedIndexExpression<K, C> value(byte[] value);

    PreparedIndexExpression<K, C> value(ByteBuffer value);

    PreparedIndexExpression<K, C> value(double value);

    PreparedIndexExpression<K, C> value(UUID value);

    <V> PreparedIndexExpression<K, C> value(V value, Serializer<V> valueSerializer);

}
