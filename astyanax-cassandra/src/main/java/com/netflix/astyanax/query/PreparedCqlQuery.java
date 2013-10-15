package com.netflix.astyanax.query;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;

import com.netflix.astyanax.Execution;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.CqlResult;

/**
 * Interface for specifying parameters on a prepared CQL query.
 * 
 * Values must be specified in the order that they were defined in the query.  
 * 
 * @author elandau
 * 
 * @param <K>
 * @param <C>
 */
public interface PreparedCqlQuery<K, C> extends Execution<CqlResult<K, C>> {
    /**
     * Specify a value of custom type for which a convenience method does not exist 
     * @param value
     * @param serializer
     */
    <V> PreparedCqlQuery<K, C> withByteBufferValue(V value, Serializer<V> serializer);

    /**
     * Set the next parameter value to this ByteBuffer
     * @param value
     */
    PreparedCqlQuery<K, C> withValue(ByteBuffer value);
    
    /**
     * Add a list of ByteBuffer values
     * @param value
     */
    PreparedCqlQuery<K, C> withValues(List<ByteBuffer> value);
    
    /**
     * Set the next parameter value to this String
     * @param value
     */
    PreparedCqlQuery<K, C> withStringValue(String value);
    
    /**
     * Set the next parameter value to this Integer
     * @param value
     */
    PreparedCqlQuery<K, C> withIntegerValue(Integer value);
    
    /**
     * Set the next parameter value to this Boolean
     * @param value
     */
    PreparedCqlQuery<K, C> withBooleanValue(Boolean value);
    
    /**
     * Set the next parameter value to this Double
     * @param value
     */
    PreparedCqlQuery<K, C> withDoubleValue(Double value);

    /**
     * Set the next parameter value to this Long
     * @param value
     */
    PreparedCqlQuery<K, C> withLongValue(Long value);

    /**
     * Set the next parameter value to this Float
     * @param value
     */
    PreparedCqlQuery<K, C> withFloatValue(Float value);

    /**
     * Set the next parameter value to this Short
     * @param value
     */
    PreparedCqlQuery<K, C> withShortValue(Short value);
    
    /**
     * Set the next parameter value to this Short
     * @param value
     */
    PreparedCqlQuery<K, C> withUUIDValue(UUID value);
}
