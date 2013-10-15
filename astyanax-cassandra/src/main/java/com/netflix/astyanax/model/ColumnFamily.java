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

import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.impl.PreparedIndexExpressionImpl;
import com.netflix.astyanax.query.PreparedIndexExpression;
import com.netflix.astyanax.serializers.ByteBufferSerializer;

/**
 * Basic column family definition. The column family definition encapsulates the
 * column family name as well as the type and serializers for the row keys and
 * first level columns. Super column subcolumn name type and serializers are
 * specified using a ColumnPath.
 * 
 * @author elandau
 * 
 * @param <K>
 * @param <C>
 */
public class ColumnFamily<K, C> implements Comparable<ColumnFamily<K,C>>{
    private final String columnFamilyName;
    private final Serializer<K> keySerializer;
    private final Serializer<C> columnSerializer;
    private final Serializer<?> defaultValueSerializer;
    private final ColumnType type;

    /**
     * @param columnFamilyName
     * @param keySerializer
     * @param columnSerializer
     * @param type
     * @deprecated Super columns should be replaced with composite columns
     */
    public ColumnFamily(String columnFamilyName, Serializer<K> keySerializer, Serializer<C> columnSerializer,
            ColumnType type) {
        this.columnFamilyName = columnFamilyName;
        this.keySerializer = keySerializer;
        this.columnSerializer = columnSerializer;
        this.defaultValueSerializer = ByteBufferSerializer.get();
        this.type = type;
    }

    public ColumnFamily(String columnFamilyName, Serializer<K> keySerializer, Serializer<C> columnSerializer) {
        this(columnFamilyName, keySerializer, columnSerializer, ByteBufferSerializer.get());
    }

    public ColumnFamily(String columnFamilyName, Serializer<K> keySerializer, Serializer<C> columnSerializer, Serializer<?> defaultValueSerializer) {
        this.columnFamilyName = columnFamilyName;
        this.keySerializer = keySerializer;
        this.columnSerializer = columnSerializer;
        this.defaultValueSerializer = defaultValueSerializer;
        this.type = ColumnType.STANDARD;
    }
    
    public String getName() {
        return columnFamilyName;
    }

    /**
     * Serializer for first level column names. This serializer does not apply
     * to sub column names.
     * 
     * @return
     */
    public Serializer<C> getColumnSerializer() {
        return columnSerializer;
    }

    /**
     * Serializer used to generate row keys.
     * 
     * @return
     */
    public Serializer<K> getKeySerializer() {
        return keySerializer;
    }
    
    public Serializer<?> getDefaultValueSerializer() {
        return defaultValueSerializer;
    }

    /**
     * Type of columns in this column family (Standard or Super)
     * 
     * @deprecated Super columns should be replaced with composite columns
     * @return
     */
    public ColumnType getType() {
        return type;
    }

    public PreparedIndexExpression<K, C> newIndexClause() {
        return new PreparedIndexExpressionImpl<K, C>(this.columnSerializer);
    }

    public static <K, C> ColumnFamily<K, C> newColumnFamily(String columnFamilyName, Serializer<K> keySerializer,
            Serializer<C> columnSerializer) {
        return new ColumnFamily<K, C>(columnFamilyName, keySerializer, columnSerializer);
    }
    
    public static <K, C> ColumnFamily<K, C> newColumnFamily(String columnFamilyName, Serializer<K> keySerializer,
            Serializer<C> columnSerializer, Serializer<?> defaultSerializer) {
        return new ColumnFamily<K, C>(columnFamilyName, keySerializer, columnSerializer, defaultSerializer);
    }

    @Override
    public int compareTo(ColumnFamily<K, C> other) {
        return getName().compareTo(other.getName());
    }
    
    @Override
    public boolean equals(Object obj) {
        ColumnFamily p = (ColumnFamily)obj;
        return this.getName().equals(p.getName());
    }
     
    @Override
    public int hashCode() {
        return this.getName().hashCode();
    }
}
