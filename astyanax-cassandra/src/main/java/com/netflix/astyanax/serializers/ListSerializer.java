package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ListType;

import com.netflix.astyanax.serializers.AbstractSerializer;

public class ListSerializer<T> extends AbstractSerializer<List<T>> {

    
    ListType<T> mySet;

    public ListSerializer(AbstractType<T> elements) {
        mySet = ListType.getInstance(elements);
    }
    
    @Override
    public List<T> fromByteBuffer(ByteBuffer arg0) {
        return mySet.compose(arg0);
    }

    @Override
    public ByteBuffer toByteBuffer(List<T> arg0) {
        return mySet.decompose(arg0);
    }
}