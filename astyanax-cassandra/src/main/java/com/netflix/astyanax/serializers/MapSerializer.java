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