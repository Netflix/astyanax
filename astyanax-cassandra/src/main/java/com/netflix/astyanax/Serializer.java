/**
 *  DataSerializer.java
 *  dsmirnov Apr 4, 2011
 */
package com.netflix.astyanax;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.netflix.astyanax.serializers.ComparatorType;

/**
 * Serializes a type T from the given bytes, or vice a versa.
 * 
 * In cassandra column names and column values (and starting with 0.7.0 row
 * keys) are all byte[]. To allow type safe conversion in java and keep all
 * conversion code in one place we define the Extractor interface. Implementors
 * of the interface define type conversion according to their domains. A
 * predefined set of common extractors can be found in the extractors package,
 * for example {@link StringSerializer}.
 * 
 * @author Ran Tavory
 * 
 * @param <T>
 *            The type to which data extraction should work.
 */
public interface Serializer<T> {

    /**
     * Extract bytes from the obj of type T
     * 
     * @param obj
     * @return
     */
    ByteBuffer toByteBuffer(T obj);

    byte[] toBytes(T obj);

    T fromBytes(byte[] bytes);

    /**
     * Extract an object of type T from the bytes.
     * 
     * @param bytes
     * @return
     */
    T fromByteBuffer(ByteBuffer byteBuffer);

    Set<ByteBuffer> toBytesSet(List<T> list);

    List<T> fromBytesSet(Set<ByteBuffer> list);

    <V> Map<ByteBuffer, V> toBytesMap(Map<T, V> map);

    <V> Map<T, V> fromBytesMap(Map<ByteBuffer, V> map);

    List<ByteBuffer> toBytesList(List<T> list);

    List<ByteBuffer> toBytesList(Collection<T> list);

    List<ByteBuffer> toBytesList(Iterable<T> list);
    
    List<T> fromBytesList(List<ByteBuffer> list);

    ComparatorType getComparatorType();

    /**
     * Return the byte buffer for the next value in sorted order for the
     * matching comparator type. This is used for paginating columns.
     * 
     * @param byteBuffer
     * @return
     */
    ByteBuffer getNext(ByteBuffer byteBuffer);

    /**
     * Create a ByteBuffer by first parsing the type out of a string
     * 
     * @param string
     * @return
     */
    ByteBuffer fromString(String string);

    String getString(ByteBuffer byteBuffer);
}
