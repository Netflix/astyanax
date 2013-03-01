package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.UTF8Type;

import com.netflix.astyanax.serializers.AbstractSerializer;

public class MapSerializer<K, V> extends AbstractSerializer<Map<K, V>> {

    MapType<K, V> myMap;

    public MapSerializer(AbstractType<K> key, AbstractType<V> value) {
        myMap = MapType.getInstance(key, value);
    }

    @Override
    public Map<K, V> fromByteBuffer(ByteBuffer arg0) {
        Map<K, V> result = arg0 == null ? null : myMap.compose(arg0);
        return result;
    }

    @Override
    public ByteBuffer toByteBuffer(Map<K, V> arg0) {
        ByteBuffer result = arg0 == null ? null : myMap.decompose(arg0);
        return result;
    }
}