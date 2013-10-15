package com.netflix.astyanax.model;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.cassandra.utils.ByteBufferUtil;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableClassToInstanceMap;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.serializers.AsciiSerializer;
import com.netflix.astyanax.serializers.BigIntegerSerializer;
import com.netflix.astyanax.serializers.BooleanSerializer;
import com.netflix.astyanax.serializers.ByteBufferOutputStream;
import com.netflix.astyanax.serializers.ByteBufferSerializer;
import com.netflix.astyanax.serializers.ComparatorType;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.SerializerTypeInferer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.serializers.UUIDSerializer;

/**
 * Parent class of Composite and DynamicComposite. Acts as a list of objects
 * that get serialized into a composite column name. Unless
 * setAutoDeserialize(true) is called, it's going to try to match serializers to
 * Cassandra comparator types.
 * 
 * @author edanuff
 */
@SuppressWarnings("rawtypes")
public abstract class AbstractComposite extends AbstractList<Object> implements Comparable<AbstractComposite> {

    public enum ComponentEquality {
        LESS_THAN_EQUAL((byte) -1), EQUAL((byte) 0), GREATER_THAN_EQUAL((byte) 1);

        private final byte equality;

        ComponentEquality(byte equality) {
            this.equality = equality;
        }

        public byte toByte() {
            return equality;
        }

        public static ComponentEquality fromByte(byte equality) {
            if (equality > 0) {
                return GREATER_THAN_EQUAL;
            }
            if (equality < 0) {
                return LESS_THAN_EQUAL;
            }
            return EQUAL;
        }
    }

    static final Logger logger = Logger.getLogger(AbstractComposite.class.getName());

    public static final BiMap<Class<? extends Serializer>, String> DEFAULT_SERIALIZER_TO_COMPARATOR_MAPPING = new ImmutableBiMap.Builder<Class<? extends Serializer>, String>()
            .put(AsciiSerializer.class,      AsciiSerializer.get().getComparatorType().getTypeName())
            .put(BigIntegerSerializer.class, BigIntegerSerializer.get().getComparatorType().getTypeName())
            .put(LongSerializer.class,       LongSerializer.get().getComparatorType().getTypeName())
            .put(IntegerSerializer.class,    IntegerSerializer.get().getComparatorType().getTypeName())
            .put(BooleanSerializer.class,    BooleanSerializer.get().getComparatorType().getTypeName())
            .put(StringSerializer.class,     StringSerializer.get().getComparatorType().getTypeName())
            .put(UUIDSerializer.class,       UUIDSerializer.get().getComparatorType().getTypeName()).build();

    static final ImmutableClassToInstanceMap<Serializer> SERIALIZERS = new ImmutableClassToInstanceMap.Builder<Serializer>()
            .put(IntegerSerializer.class,    IntegerSerializer.get())
            .put(BooleanSerializer.class,    BooleanSerializer.get())
            .put(AsciiSerializer.class,      AsciiSerializer.get())
            .put(BigIntegerSerializer.class, BigIntegerSerializer.get())
            .put(ByteBufferSerializer.class, ByteBufferSerializer.get())
            .put(LongSerializer.class,       LongSerializer.get())
            .put(StringSerializer.class,     StringSerializer.get())
            .put(UUIDSerializer.class,       UUIDSerializer.get()).build();

    public static final BiMap<Byte, String> DEFAULT_ALIAS_TO_COMPARATOR_MAPPING = new ImmutableBiMap.Builder<Byte, String>()
            .put((byte) 'a', ComparatorType.ASCIITYPE.getTypeName())
            .put((byte) 'b', ComparatorType.BYTESTYPE.getTypeName())
            .put((byte) 'i', ComparatorType.INTEGERTYPE.getTypeName())
            .put((byte) 'x', ComparatorType.LEXICALUUIDTYPE.getTypeName())
            .put((byte) 'l', ComparatorType.LONGTYPE.getTypeName())
            .put((byte) 't', ComparatorType.TIMEUUIDTYPE.getTypeName())
            .put((byte) 's', ComparatorType.UTF8TYPE.getTypeName())
            .put((byte) 'u', ComparatorType.UUIDTYPE.getTypeName()).build();

    BiMap<Class<? extends Serializer>, String> serializerToComparatorMapping = DEFAULT_SERIALIZER_TO_COMPARATOR_MAPPING;

    BiMap<Byte, String> aliasToComparatorMapping = DEFAULT_ALIAS_TO_COMPARATOR_MAPPING;

    final boolean dynamic;

    List<Serializer<?>> serializersByPosition = null;
    List<String> comparatorsByPosition = null;

    public class Component<T> {
        final Serializer<T> serializer;
        final T value;
        final ByteBuffer bytes;
        final String comparator;
        final ComponentEquality equality;

        public Component(T value, ByteBuffer bytes, Serializer<T> serializer, String comparator,
                ComponentEquality equality) {
            this.serializer = serializer;
            this.value = value;
            this.bytes = bytes;
            this.comparator = comparator;
            this.equality = equality;
        }

        public Serializer<T> getSerializer() {
            return serializer;
        }

        @SuppressWarnings("unchecked")
        public <A> A getValue(Serializer<A> s) {
            if (s == null) {
                s = (Serializer<A>) serializer;
            }
            if ((value == null) && (bytes != null) && (s != null)) {
                ByteBuffer cb = bytes.duplicate();
                if (cb.hasRemaining()) {
                    return s.fromByteBuffer(cb);
                }
            }
            if (value instanceof ByteBuffer) {
                return (A) ((ByteBuffer) value).duplicate();
            }
            return (A) value;
        }

        public T getValue() {
            return getValue(serializer);
        }

        @SuppressWarnings("unchecked")
        public <A> ByteBuffer getBytes(Serializer<A> s) {
            if (bytes == null) {
                if (value instanceof ByteBuffer) {
                    return ((ByteBuffer) value).duplicate();
                }

                if (value == null) {
                    return null;
                }

                if (s == null) {
                    s = (Serializer<A>) serializer;
                }
                if (s != null) {
                    return s.toByteBuffer((A) value).duplicate();
                }

            }

            return bytes.duplicate();
        }

        public ByteBuffer getBytes() {
            return getBytes(serializer);
        }

        public String getComparator() {
            return comparator;
        }

        public ComponentEquality getEquality() {
            return equality;
        }

        @Override
        public String toString() {
            return "Component [" + getValue() + "]";
        }
    }

    List<Component<?>> components = new ArrayList<Component<?>>();

    ByteBuffer serialized = null;

    public AbstractComposite(boolean dynamic) {
        this.dynamic = dynamic;
    }

    public AbstractComposite(boolean dynamic, Object... o) {
        this.dynamic = dynamic;
        this.addAll(Arrays.asList(o));
    }

    public AbstractComposite(boolean dynamic, List<?> l) {
        this.dynamic = dynamic;
        this.addAll(l);
    }

    public List<Component<?>> getComponents() {
        return components;
    }

    public void setComponents(List<Component<?>> components) {
        serialized = null;
        this.components = components;
    }

    public Map<Class<? extends Serializer>, String> getSerializerToComparatorMapping() {
        return serializerToComparatorMapping;
    }

    public void setSerializerToComparatorMapping(Map<Class<? extends Serializer>, String> serializerToComparatorMapping) {
        serialized = null;
        this.serializerToComparatorMapping = new ImmutableBiMap.Builder<Class<? extends Serializer>, String>().putAll(
                serializerToComparatorMapping).build();
    }

    public Map<Byte, String> getAliasesToComparatorMapping() {
        return aliasToComparatorMapping;
    }

    public void setAliasesToComparatorMapping(Map<Byte, String> aliasesToComparatorMapping) {
        serialized = null;
        aliasToComparatorMapping = new ImmutableBiMap.Builder<Byte, String>().putAll(aliasesToComparatorMapping)
                .build();
    }

    public boolean isDynamic() {
        return dynamic;
    }

    public List<Serializer<?>> getSerializersByPosition() {
        return serializersByPosition;
    }

    public void setSerializersByPosition(List<Serializer<?>> serializersByPosition) {
        this.serializersByPosition = serializersByPosition;
    }

    public void setSerializersByPosition(Serializer<?>... serializers) {
        serializersByPosition = Arrays.asList(serializers);
    }

    public void setSerializerByPosition(int index, Serializer<?> s) {
        if (serializersByPosition == null) {
            serializersByPosition = new ArrayList<Serializer<?>>();
        }
        while (serializersByPosition.size() <= index) {
            serializersByPosition.add(null);
        }
        serializersByPosition.set(index, s);
    }

    public List<String> getComparatorsByPosition() {
        return comparatorsByPosition;
    }

    public void setComparatorsByPosition(List<String> comparatorsByPosition) {
        this.comparatorsByPosition = comparatorsByPosition;
    }

    public void setComparatorsByPosition(String... comparators) {
        comparatorsByPosition = Arrays.asList(comparators);
    }

    public void setComparatorByPosition(int index, String c) {
        if (comparatorsByPosition == null) {
            comparatorsByPosition = new ArrayList<String>();
        }
        while (comparatorsByPosition.size() <= index) {
            comparatorsByPosition.add(null);
        }
        comparatorsByPosition.set(index, c);
    }

    @Override
    public int compareTo(AbstractComposite o) {
        return serialize().compareTo(o.serialize());
    }

    private String comparatorForSerializer(Serializer<?> s) {
        String comparator = serializerToComparatorMapping.get(s.getClass());
        if (comparator != null) {
            return comparator;
        }
        return ComparatorType.BYTESTYPE.getTypeName();
    }

    private Serializer<?> serializerForComparator(String c) {
        int p = c.indexOf('(');
        if (p >= 0) {
            c = c.substring(0, p);
        }
        if (ComparatorType.LEXICALUUIDTYPE.getTypeName().equals(c)
                || ComparatorType.TIMEUUIDTYPE.getTypeName().equals(c)) {
            return UUIDSerializer.get();
        }

        Serializer<?> s = SERIALIZERS.getInstance(serializerToComparatorMapping.inverse().get(c));
        if (s != null) {
            return s;
        }
        return ByteBufferSerializer.get();
    }

    private Serializer<?> serializerForPosition(int i) {
        if (serializersByPosition == null) {
            return null;
        }
        if (i >= serializersByPosition.size()) {
            return null;
        }
        return serializersByPosition.get(i);
    }

    private Serializer<?> getSerializer(int i, String c) {
        Serializer<?> s = serializerForPosition(i);
        if (s != null) {
            return s;
        }
        return serializerForComparator(c);
    }

    private String comparatorForPosition(int i) {
        if (comparatorsByPosition == null) {
            return null;
        }
        if (i >= comparatorsByPosition.size()) {
            return null;
        }
        return comparatorsByPosition.get(i);
    }

    private String getComparator(int i, ByteBuffer bb) {
        String name = comparatorForPosition(i);
        if (name != null) {
            return name;
        }
        if (!dynamic) {
            if (bb.hasRemaining()) {
                return ComparatorType.BYTESTYPE.getTypeName();
            }
            else {
                return null;
            }
        }
        if (bb.hasRemaining()) {
            try {
                int header = getShortLength(bb);
                if ((header & 0x8000) == 0) {
                    name = ByteBufferUtil.string(getBytes(bb, header));
                }
                else {
                    byte a = (byte) (header & 0xFF);
                    name = aliasToComparatorMapping.get(a);
                    if (name == null) {
                        a = (byte) Character.toUpperCase((char) a);
                        name = aliasToComparatorMapping.get(a);
                        if (name != null) {
                            name += "(reversed=true)";
                        }
                    }
                }
            }
            catch (CharacterCodingException e) {
                throw new RuntimeException(e);
            }
        }
        if ((name != null) && (name.length() == 0)) {
            name = null;
        }
        return name;
    }

    @Override
    public void clear() {
        serialized = null;
        components = new ArrayList<Component<?>>();
    }

    @Override
    public int size() {
        return components.size();
    }

    public <T> AbstractComposite addComponent(T value, Serializer<T> s) {

        addComponent(value, s, comparatorForSerializer(s));

        return this;

    }

    public <T> AbstractComposite addComponent(T value, Serializer<T> s, ComponentEquality equality) {

        addComponent(value, s, comparatorForSerializer(s), equality);

        return this;

    }

    public <T> AbstractComposite addComponent(T value, Serializer<T> s, String comparator) {

        addComponent(value, s, comparator, ComponentEquality.EQUAL);

        return this;

    }

    public <T> AbstractComposite addComponent(T value, Serializer<T> s, String comparator, ComponentEquality equality) {

        addComponent(-1, value, s, comparator, equality);

        return this;

    }

    @SuppressWarnings("unchecked")
    public <T> AbstractComposite addComponent(int index, T value, Serializer<T> s, String comparator,
            ComponentEquality equality) {
        serialized = null;

        if (index < 0) {
            index = components.size();
        }

        while (components.size() < index) {
            components.add(null);
        }
        components.add(index, new Component(value, null, s, comparator, equality));

        return this;

    }

    private static Object mapIfNumber(Object o) {
        if ((o instanceof Byte) || (o instanceof Integer) || (o instanceof Short)) {
            return BigInteger.valueOf(((Number) o).longValue());
        }
        return o;
    }

    @SuppressWarnings({ "unchecked" })
    private static Collection<?> flatten(Collection<?> c) {
        if (c instanceof AbstractComposite) {
            return ((AbstractComposite) c).getComponents();
        }
        boolean hasCollection = false;
        for (Object o : c) {
            if (o instanceof Collection) {
                hasCollection = true;
                break;
            }
        }
        if (!hasCollection) {
            return c;
        }
        List newList = new ArrayList();
        for (Object o : c) {
            if (o instanceof Collection) {
                newList.addAll(flatten((Collection) o));
            }
            else {
                newList.add(o);
            }
        }
        return newList;
    }

    @Override
    public boolean addAll(Collection<? extends Object> c) {
        return super.addAll(flatten(c));
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return super.containsAll(flatten(c));
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return super.removeAll(flatten(c));
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return super.retainAll(flatten(c));
    }

    @Override
    public boolean addAll(int i, Collection<? extends Object> c) {
        return super.addAll(i, flatten(c));
    }

    @SuppressWarnings("unchecked")
    @Override
    public void add(int index, Object element) {
        serialized = null;

        if (element instanceof Component) {
            components.add(index, (Component<?>) element);
            return;
        }

        element = mapIfNumber(element);
        Serializer s = serializerForPosition(index);
        if (s == null) {
            s = SerializerTypeInferer.getSerializer(element);
        }
        String c = comparatorForPosition(index);
        if (c == null) {
            c = comparatorForSerializer(s);
        }
        components.add(index, new Component(element, null, s, c, ComponentEquality.EQUAL));
    }

    @Override
    public Object remove(int index) {
        serialized = null;
        Component prev = components.remove(index);
        if (prev != null) {
            return prev.getValue();
        }
        return null;
    }

    public <T> AbstractComposite setComponent(int index, T value, Serializer<T> s) {

        setComponent(index, value, s, comparatorForSerializer(s));

        return this;

    }

    public <T> AbstractComposite setComponent(int index, T value, Serializer<T> s, String comparator) {

        setComponent(index, value, s, comparator, ComponentEquality.EQUAL);

        return this;

    }

    @SuppressWarnings("unchecked")
    public <T> AbstractComposite setComponent(int index, T value, Serializer<T> s, String comparator,
            ComponentEquality equality) {
        serialized = null;

        while (components.size() <= index) {
            components.add(null);
        }
        components.set(index, new Component(value, null, s, comparator, equality));

        return this;

    }

    @SuppressWarnings("unchecked")
    @Override
    public Object set(int index, Object element) {
        serialized = null;

        if (element instanceof Component) {
            Component prev = components.set(index, (Component<?>) element);
            if (prev != null) {
                return prev.getValue();
            }
            return null;
        }

        element = mapIfNumber(element);
        Serializer s = serializerForPosition(index);
        if (s == null) {
            s = SerializerTypeInferer.getSerializer(element);
        }
        String c = comparatorForPosition(index);
        if (c == null) {
            c = comparatorForSerializer(s);
        }
        Component prev = components.set(index, new Component(element, null, s, c, ComponentEquality.EQUAL));
        if (prev != null) {
            return prev.getValue();
        }
        return null;
    }

    @Override
    public Object get(int i) {
        Component c = components.get(i);
        if (c != null) {
            return c.getValue();
        }
        return null;
    }

    public <T> T get(int i, Serializer<T> s) throws ClassCastException {
        T value = null;
        Component<?> c = components.get(i);
        if (c != null) {
            value = c.getValue(s);
        }
        return value;
    }

    public Component getComponent(int i) {
        if (i >= components.size()) {
            return null;
        }
        Component c = components.get(i);
        return c;
    }

    public Iterator<Component<?>> componentsIterator() {
        return components.iterator();
    }

    @SuppressWarnings("unchecked")
    public ByteBuffer serialize() {
        if (serialized != null) {
            return serialized.duplicate();
        }

        ByteBufferOutputStream out = new ByteBufferOutputStream();

        int i = 0;
        for (Component c : components) {
            Serializer<?> s = serializerForPosition(i);
            ByteBuffer cb = c.getBytes(s);
            if (cb == null) {
                cb = ByteBuffer.allocate(0);
            }

            if (dynamic) {
                String comparator = comparatorForPosition(i);
                if (comparator == null) {
                    comparator = c.getComparator();
                }
                if (comparator == null) {
                    comparator = ComparatorType.BYTESTYPE.getTypeName();
                }
                int p = comparator.indexOf("(reversed=true)");
                boolean desc = false;
                if (p >= 0) {
                    comparator = comparator.substring(0, p);
                    desc = true;
                }
                if (aliasToComparatorMapping.inverse().containsKey(comparator)) {
                    byte a = aliasToComparatorMapping.inverse().get(comparator);
                    if (desc) {
                        a = (byte) Character.toUpperCase((char) a);
                    }
                    out.writeShort((short) (0x8000 | a));
                }
                else {
                    out.writeShort((short) comparator.length());
                    out.write(ByteBufferUtil.bytes(comparator));
                }
            }
            out.writeShort((short) cb.remaining());
            out.write(cb.slice());
            out.write(c.getEquality().toByte());
            i++;
        }

        serialized = out.getByteBuffer();
        return serialized.duplicate();
    }

    @SuppressWarnings("unchecked")
    public void deserialize(ByteBuffer b) {
        serialized = b.duplicate();
        components = new ArrayList<Component<?>>();

        String comparator = null;
        int i = 0;
        while ((comparator = getComparator(i, b)) != null) {
            ByteBuffer data = getWithShortLength(b);
            if (data != null) {
                Serializer<?> s = getSerializer(i, comparator);
                ComponentEquality equality = ComponentEquality.fromByte(b.get());
                components.add(new Component(null, data.slice(), s, comparator, equality));
            }
            else {
                throw new RuntimeException("Missing component data in composite type");
            }
            i++;
        }

    }

    protected static int getShortLength(ByteBuffer bb) {
        int length = (bb.get() & 0xFF) << 8;
        return length | (bb.get() & 0xFF);
    }

    protected static ByteBuffer getBytes(ByteBuffer bb, int length) {
        ByteBuffer copy = bb.duplicate();
        copy.limit(copy.position() + length);
        bb.position(bb.position() + length);
        return copy;
    }

    protected static ByteBuffer getWithShortLength(ByteBuffer bb) {
        int length = getShortLength(bb);
        return getBytes(bb, length);
    }

}
