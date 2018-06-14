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
package com.netflix.astyanax;

import java.nio.ByteBuffer;
import java.util.Set;

/**
 * Grouping of serializers for a single column family. Use this only for
 * implementing advanced data models.
 * 
 * @author elandau
 * 
 */
public interface SerializerPackage {
    /**
     * @return Return the serializer for keys
     */
    Serializer<?> getKeySerializer();

    /**
     * @deprecated use getColumnNameSerializer()
     */
    @Deprecated
    Serializer<?> getColumnSerializer();

    /**
     * @return Return serializer for column names
     */
    Serializer<?> getColumnNameSerializer();

    /**
     * @deprecated use getDefaultValueSerializer()
     */
    @Deprecated
    Serializer<?> getValueSerializer();

    /**
     * @return Return the default value serializer
     */
    Serializer<?> getDefaultValueSerializer();

    /**
     * @deprecated use getColumnSerializer()
     */
    @Deprecated
    Serializer<?> getValueSerializer(ByteBuffer columnName);

    /**
     * @return  Return the value serializer for the specified column name
     * 
     * @param columnName
     */
    Serializer<?> getColumnSerializer(ByteBuffer columnName);

    /**
     * @deprecated use getColumnSerializer
     */
    @Deprecated
    Serializer<?> getValueSerializer(String columnName);

    /**
     * @return Return the value serializer for the specified column name
     * 
     * @param columnName
     */
    Serializer<?> getColumnSerializer(String columnName);

    /**
     * @return Return the set of supported column names
     */
    Set<ByteBuffer> getColumnNames();

    /**
     * @return Convert a key to a string using the package's key serializer
     * 
     * @param key
     */
    String keyAsString(ByteBuffer key);

    /**
     * Convert a column name to a string using the package's column serializer
     * 
     * @param key
     */
    String columnAsString(ByteBuffer column);

    /**
     * Convert a value to a string using the package's value serializer. Will
     * use either a column specific serializer, if one was specified, or the
     * default value serializer.
     * 
     * @param key
     */
    String valueAsString(ByteBuffer column, ByteBuffer value);

    /**
     * Convert a string key to a ByteBuffer using the package's key serializer
     * 
     * @param key
     */
    ByteBuffer keyAsByteBuffer(String key);

    /**
     * Convert a string column name to a ByteBuffer using the package's column
     * serializer
     * 
     * @param key
     */
    ByteBuffer columnAsByteBuffer(String column);

    /**
     * Convert a string value to a string using the package's value serializer
     * 
     * @param key
     */
    ByteBuffer valueAsByteBuffer(ByteBuffer column, String value);

    /**
     * Convert a string value to a string using the package's value serializer
     * 
     * @param column
     * @param value
     */
    ByteBuffer valueAsByteBuffer(String column, String value);
}
