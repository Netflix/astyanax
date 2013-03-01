package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.UTF8Type;

import com.netflix.astyanax.serializers.AbstractSerializer;

public class MapSerializer<K, V> extends AbstractSerializer<Map<K, V>> {

    MapType<K, V> mySet;

    public MapSerializer(AbstractType<K> key, AbstractType<V> value) {
        mySet = MapType.getInstance(key, value);
    }

    @Override
    public Map<K, V> fromByteBuffer(ByteBuffer arg0) {
        return mySet.compose(arg0);
    }

    @Override
    public ByteBuffer toByteBuffer(Map<K, V> arg0) {
        return mySet.decompose(arg0);
    }
}