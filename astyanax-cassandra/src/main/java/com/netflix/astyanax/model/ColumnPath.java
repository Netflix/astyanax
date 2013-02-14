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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.serializers.DoubleSerializer;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

/**
 * Container for a path within a row. The path is essentially a list of columns
 * in hierarchical order. Paths can have any column name type which is
 * eventually converted to a ByteBuffer.
 * 
 * When querying a super column the path must also include a serializer for the
 * sub columns names. The serializer is not needed when reading a subcolumn or
 * standard column.
 * 
 * The current Cassandra implementation only supports a path depth of 2.
 * 
 * C - Serializer for column names at the end of the path. For super columns. C2
 * - Serializer for a column name that is part of the path
 * 
 * @author elandau
 * 
 *         TODO: Add append for all serializer types
 * @deprecated Super columns should be replaced with composite columns
 */
public class ColumnPath<C> implements Iterable<ByteBuffer> {
    private List<ByteBuffer> path = new ArrayList<ByteBuffer>();
    private Serializer<C> columnSerializer;

    /**
     * Construct an empty path and give it the serializer for column names at
     * the end of the path. Use this constructor when performing a query
     * 
     * @param columnSerializer
     */
    public ColumnPath(Serializer<C> columnSerializer) {
        this.columnSerializer = columnSerializer;
    }

    /**
     * Construct a column path for a mutation. The serializer for the column
     * names at the end of the path is not necessary.
     */
    public ColumnPath() {
    }

    /**
     * Add a depth to the path
     * 
     * @param <C>
     * @param ser
     * @param name
     * @return
     */
    public <C2> ColumnPath<C> append(C2 name, Serializer<C2> ser) {
        path.add(ByteBuffer.wrap(ser.toBytes(name)));
        return this;
    }

    public <C2> ColumnPath<C> append(String name) {
        append(name, StringSerializer.get());
        return this;
    }

    public <C2> ColumnPath<C> append(int name) {
        append(name, IntegerSerializer.get());
        return this;
    }

    public <C2> ColumnPath<C> append(double name) {
        append(name, DoubleSerializer.get());
        return this;
    }

    @Override
    public Iterator<ByteBuffer> iterator() {
        return path.iterator();
    }

    /**
     * Return the path 'depth'
     * 
     * @return
     */
    public int length() {
        return path.size();
    }

    /**
     * Get a path element from a specific depth
     * 
     * @param index
     * @return
     */
    public ByteBuffer get(int index) {
        return path.get(index);
    }

    /**
     * Returns the last element in the path. This is usually the column name
     * being queried or modified.
     * 
     * @return
     */
    public ByteBuffer getLast() {
        return path.get(path.size() - 1);
    }

    /**
     * Return serializer for column names at the end of the path
     * 
     * @return
     */
    public Serializer<C> getSerializer() {
        return this.columnSerializer;
    }
}
