package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;
import java.util.Set;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;

import com.netflix.astyanax.serializers.AbstractSerializer;

/**
 * Serializer implementation for generic sets.
 * 
 * @author vermes
 * 
 * @param <T>
 *            element type
 */
public class SetSerializer<T> extends AbstractSerializer<Set<T>> {

    private SetType<T> mySet;

    /**
     * @param elements
     */
    public SetSerializer(AbstractType<T> elements) {
        mySet = SetType.getInstance(elements);
    }

    @Override
    public Set<T> fromByteBuffer(ByteBuffer arg0) {
        Set<T> result = arg0 == null ? null : mySet.compose(arg0);
        return result;
    }

    @Override
    public ByteBuffer toByteBuffer(Set<T> arg0) {
        ByteBuffer result = arg0 == null ? null : mySet.decompose(arg0);
        return result;
    }
}