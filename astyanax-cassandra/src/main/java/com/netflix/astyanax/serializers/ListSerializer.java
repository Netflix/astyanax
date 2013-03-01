package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ListType;

import com.netflix.astyanax.serializers.AbstractSerializer;

/**
 * Serializer implementation for generic lists.
 * 
 * @author vermes
 * 
 * @param <T>
 *            element type
 */
public class ListSerializer<T> extends AbstractSerializer<List<T>> {

    private ListType<T> myList;

    /**
     * @param elements
     */
    public ListSerializer(AbstractType<T> elements) {
        myList = ListType.getInstance(elements);
    }

    @Override
    public List<T> fromByteBuffer(ByteBuffer arg0) {
        return arg0 == null ? null : myList.compose(arg0);
    }

    @Override
    public ByteBuffer toByteBuffer(List<T> arg0) {
        return arg0 == null ? null : myList.decompose(arg0);
    }
}