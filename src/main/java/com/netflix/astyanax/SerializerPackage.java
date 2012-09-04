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
     * Return the serializer for keys
     *
     * @return
     */
    Serializer<?> getKeySerializer();

    /**
     * @deprecated use getColumnNameSerializer()
     */
    @Deprecated
    Serializer<?> getColumnSerializer();

    /**
     * Return serializer for column names
     *
     * @author elandau
     * @return
     */
    Serializer<?> getColumnNameSerializer();

    /**
     * @deprecated use getDefaultValueSerializer()
     * @return
     */
    @Deprecated
    Serializer<?> getValueSerializer();

    /**
     * Return the default value serializer
     *
     * @return
     */
    Serializer<?> getDefaultValueSerializer();

    /**
     * @deprecated use getColumnSerializer()
     */
    @Deprecated
    Serializer<?> getValueSerializer(ByteBuffer columnName);

    /**
     * Return the value serializer for the specified column name
     *
     * @param columnName
     * @return
     */
    Serializer<?> getColumnSerializer(ByteBuffer columnName);

    /**
     * @deprecated use getColumnSerializer
     */
    @Deprecated
    Serializer<?> getValueSerializer(String columnName);

    /**
     * Return the value serializer for the specified column name
     *
     * @param columnName
     * @return
     */
    Serializer<?> getColumnSerializer(String columnName);

    /**
     * Return the set of supported column names
     *
     * @return
     */
    Set<ByteBuffer> getColumnNames();

    /**
     * Convert a key to a string using the package's key serializer
     *
     * @param key
     * @return
     */
    String keyAsString(ByteBuffer key);

    /**
     * Convert a column name to a string using the package's column serializer
     *
     * @param key
     * @return
     */
    String columnAsString(ByteBuffer column);

    /**
     * Convert a value to a string using the package's value serializer. Will
     * use either a column specific serializer, if one was specified, or the
     * default value serializer.
     *
     * @param key
     * @return
     */
    String valueAsString(ByteBuffer column, ByteBuffer value);

    /**
     * Convert a string key to a ByteBuffer using the package's key serializer
     *
     * @param key
     * @return
     */
    ByteBuffer keyAsByteBuffer(String key);

    /**
     * Convert a string column name to a ByteBuffer using the package's column
     * serializer
     *
     * @param key
     * @return
     */
    ByteBuffer columnAsByteBuffer(String column);

    /**
     * Convert a string value to a string using the package's value serializer
     *
     * @param key
     * @return
     */
    ByteBuffer valueAsByteBuffer(ByteBuffer column, String value);

    /**
     * Convert a string value to a string using the package's value serializer
     *
     * @param column
     * @param value
     * @return
     */
    ByteBuffer valueAsByteBuffer(String column, String value);
}
