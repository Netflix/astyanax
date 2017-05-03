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
import java.util.Map;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MapType;

import com.netflix.astyanax.serializers.AbstractSerializer;

/**
 * Serializer implementation for generic maps.
 * 
 * @author vermes
 * 
 * @param <K>
 *            key type
 * @param <V>
 *            value type
 */
public class MapSerializer<K, V> extends AbstractSerializer<Map<K, V>> {

    private final MapType<K, V> myMap;

    /**
     * @param key
     * @param value
     */
    public MapSerializer(AbstractType<K> key, AbstractType<V> value) {
        myMap = MapType.getInstance(key, value);
    }

    @Override
    public Map<K, V> fromByteBuffer(ByteBuffer arg0) {
        if (arg0 == null) return null;
            ByteBuffer dup = arg0.duplicate();
            return myMap.compose(dup);
            }

    @Override
    public ByteBuffer toByteBuffer(Map<K, V> arg0) {
        return arg0 == null ? null : myMap.decompose(arg0);
    }
}