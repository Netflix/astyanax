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

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ListType;

import com.netflix.astyanax.serializers.AbstractSerializer;

/**
 * Serializer implementation for generic lists.
 * 
 * @author vermes
 * 
 * @param <T>
 *            element type
 */
public class ListSerializer<T> extends AbstractSerializer<List<T>> {

    private final ListType<T> myList;

    /**
     * @param elements
     */
    public ListSerializer(AbstractType<T> elements) {
        myList = ListType.getInstance(elements);
    }

    @Override
    public List<T> fromByteBuffer(ByteBuffer arg0) {
        if (arg0 ==  null) return null;
        ByteBuffer dup = arg0.duplicate();
        return myList.compose(dup);
    }

    @Override
    public ByteBuffer toByteBuffer(List<T> arg0) {
        return arg0 == null ? null : myList.decompose(arg0);
    }
}