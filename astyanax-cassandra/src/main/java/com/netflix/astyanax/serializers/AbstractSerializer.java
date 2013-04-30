package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.netflix.astyanax.Serializer;

/**
 * A base class for serializer implementations. Takes care of the default
 * implementations of to/fromBytesList and to/fromBytesMap. Extenders of this
 * class only need to implement toByteBuffer and fromByteBuffer.
 * 
 * @author Ed Anuff
 * 
 * @param <T>
 */
public abstract class AbstractSerializer<T> implements Serializer<T> {

    @Override
    public abstract ByteBuffer toByteBuffer(T obj);

    @Override
    public byte[] toBytes(T obj) {
        ByteBuffer bb = toByteBuffer(obj);
        byte[] bytes = new byte[bb.remaining()];
        bb.get(bytes, 0, bytes.length);
        return bytes;
    }

    @Override
    public T fromBytes(byte[] bytes) {
        return fromByteBuffer(ByteBuffer.wrap(bytes));
    }

    /*
     * public ByteBuffer toByteBuffer(T obj) { return
     * ByteBuffer.wrap(toBytes(obj)); }
     * 
     * public ByteBuffer toByteBuffer(T obj, ByteBuffer byteBuffer, int offset,
     * int length) { byteBuffer.put(toBytes(obj), offset, length); return
     * byteBuffer; }
     */

    @Override
    public abstract T fromByteBuffer(ByteBuffer byteBuffer);

    /*
     * public T fromByteBuffer(ByteBuffer byteBuffer) { return
     * fromBytes(byteBuffer.array()); }
     * 
     * public T fromByteBuffer(ByteBuffer byteBuffer, int offset, int length) {
     * return fromBytes(Arrays.copyOfRange(byteBuffer.array(), offset, length));
     * }
     */

    public Set<ByteBuffer> toBytesSet(List<T> list) {
        Set<ByteBuffer> bytesList = new HashSet<ByteBuffer>(computeInitialHashSize(list.size()));
        for (T s : list) {
            bytesList.add(toByteBuffer(s));
        }
        return bytesList;
    }

    public List<T> fromBytesSet(Set<ByteBuffer> set) {
        List<T> objList = new ArrayList<T>(set.size());
        for (ByteBuffer b : set) {
            objList.add(fromByteBuffer(b));
        }
        return objList;
    }

    public List<ByteBuffer> toBytesList(List<T> list) {
        return Lists.transform(list, new Function<T, ByteBuffer>() {
            @Override
            public ByteBuffer apply(T s) {
                return toByteBuffer(s);
            }
        });
    }

    public List<ByteBuffer> toBytesList(Collection<T> list) {
        List<ByteBuffer> bytesList = new ArrayList<ByteBuffer>(list.size());
        for (T s : list) {
            bytesList.add(toByteBuffer(s));
        }
        return bytesList;
    }

    public List<ByteBuffer> toBytesList(Iterable<T> list) {
        return Lists.newArrayList(Iterables.transform(list, new Function<T, ByteBuffer>() {
            @Override
            public ByteBuffer apply(T s) {
                return toByteBuffer(s);
            }
        }));
    }

    public List<T> fromBytesList(List<ByteBuffer> list) {
        List<T> objList = new ArrayList<T>(list.size());
        for (ByteBuffer s : list) {
            objList.add(fromByteBuffer(s));
        }
        return objList;
    }

    public <V> Map<ByteBuffer, V> toBytesMap(Map<T, V> map) {
        Map<ByteBuffer, V> bytesMap = new LinkedHashMap<ByteBuffer, V>(computeInitialHashSize(map.size()));
        for (Entry<T, V> entry : map.entrySet()) {
            bytesMap.put(toByteBuffer(entry.getKey()), entry.getValue());
        }
        return bytesMap;
    }

    public <V> Map<T, V> fromBytesMap(Map<ByteBuffer, V> map) {
        Map<T, V> objMap = new LinkedHashMap<T, V>(computeInitialHashSize(map.size()));
        for (Entry<ByteBuffer, V> entry : map.entrySet()) {
            objMap.put(fromByteBuffer(entry.getKey()), entry.getValue());
        }
        return objMap;
    }

    public int computeInitialHashSize(int initialSize) {
        return Double.valueOf(Math.floor(initialSize / 0.75)).intValue() + 1;
    }

    public ComparatorType getComparatorType() {
        return ComparatorType.BYTESTYPE;
    }

    @Override
    public ByteBuffer fromString(String string) {
        throw new UnsupportedOperationException(this.getClass().getCanonicalName());
    }

    @Override
    public String getString(ByteBuffer byteBuffer) {
        throw new UnsupportedOperationException(this.getClass().getCanonicalName());
    }

    @Override
    public ByteBuffer getNext(ByteBuffer byteBuffer) {
        ByteBufferOutputStream next = new ByteBufferOutputStream();
        try {
            next.write(byteBuffer);
            next.write((byte) 0);
            return next.getByteBuffer();
        }
        finally {
            try {
                next.close();
            }
            catch (Throwable t) {
            }
        }
    }
}
