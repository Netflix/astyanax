package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;
import java.util.Set;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;

import com.netflix.astyanax.serializers.AbstractSerializer;

public class SetSerializer<T> extends AbstractSerializer<Set<T>> {

    SetType<T> mySet;

    public SetSerializer(AbstractType<T> elements) {
        mySet = SetType.getInstance(elements);
    }

    @Override
    public Set<T> fromByteBuffer(ByteBuffer arg0) {
        return mySet.compose(arg0);
    }

    @Override
    public ByteBuffer toByteBuffer(Set<T> arg0) {
        return mySet.decompose(arg0);
    }
}