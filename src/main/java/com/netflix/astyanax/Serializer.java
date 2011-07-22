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
 *          The type to which data extraction should work.
 */
public interface Serializer<T> {

  /**
   * Extract bytes from the obj of type T
   * 
   * @param obj
   * @return
   */
  public ByteBuffer toByteBuffer(T obj);

  public byte[] toBytes(T obj);

  public T fromBytes(byte[] bytes);

  /**
   * Extract an object of type T from the bytes.
   * 
   * @param bytes
   * @return
   */
  public T fromByteBuffer(ByteBuffer byteBuffer);

  public Set<ByteBuffer> toBytesSet(List<T> list);

  public List<T> fromBytesSet(Set<ByteBuffer> list);

  public <V> Map<ByteBuffer, V> toBytesMap(Map<T, V> map);

  public <V> Map<T, V> fromBytesMap(Map<ByteBuffer, V> map);

  public List<ByteBuffer> toBytesList(List<T> list);

  public List<ByteBuffer> toBytesList(Collection<T> list);
  
  public List<T> fromBytesList(List<ByteBuffer> list);

  public ComparatorType getComparatorType();

}
