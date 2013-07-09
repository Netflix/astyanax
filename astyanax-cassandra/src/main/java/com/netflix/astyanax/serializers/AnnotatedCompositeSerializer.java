package com.netflix.astyanax.serializers;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.annotations.Component;
import com.netflix.astyanax.model.Equality;
import com.netflix.astyanax.model.RangeEndpoint;
import java.util.Arrays;

/**
 * Serializer for a Pojo annotated with Component field annotations
 * 
 * Serialized data is formatted as a list of components with each component
 * having the format: <2 byte length><data><0>
 * 
 * @author elandau
 * 
 * @param <T>
 */
public class AnnotatedCompositeSerializer<T> extends AbstractSerializer<T> {
    private static final byte       END_OF_COMPONENT    = 0;
    private static final ByteBuffer EMPTY_BYTE_BUFFER   = ByteBuffer.allocate(0);
    private static final int        DEFAULT_BUFFER_SIZE = 128;
    private static final int        COMPONENT_OVERHEAD  = 3;
    
    /**
     * Serializer for a single component within the Pojo
     * 
     * @author elandau
     * 
     * @param <P>
     */
    public static class ComponentSerializer<P> implements Comparable<ComponentSerializer<?>> {
        private Field field;
        private Serializer<P> serializer;
        private int ordinal;

        public ComponentSerializer(Field field, Serializer<P> serializer, int ordinal) {
            this.field = field;
            this.field.setAccessible(true);
            this.serializer = serializer;
            this.ordinal = ordinal;
        }

        public Field getField() {
            return this.field;
        }

        public ByteBuffer serialize(Object obj) throws IllegalArgumentException, IllegalAccessException {
            Object value = field.get(obj);
            ByteBuffer buf = serializer.toByteBuffer((P) value);
            return buf;
        }

        public void deserialize(Object obj, ByteBuffer value) throws IllegalArgumentException, IllegalAccessException {
           	field.set(obj, serializer.fromByteBuffer(value));
        }

        public ByteBuffer serializeValue(Object value) {
            ByteBuffer buf = serializer.toByteBuffer((P) value);
            return buf;
        }

        @Override
        public int compareTo(ComponentSerializer<?> other) {
            return this.ordinal - other.ordinal;
        }
    }

    private final List<ComponentSerializer<?>> components;
    private final Class<T> clazz;
    private final int bufferSize;
    
    public AnnotatedCompositeSerializer(Class<T> clazz, boolean includeParentFields) {
        this(clazz, DEFAULT_BUFFER_SIZE, includeParentFields);
	}

    public AnnotatedCompositeSerializer(Class<T> clazz) {
        this(clazz, DEFAULT_BUFFER_SIZE, false);
    }
    
    public AnnotatedCompositeSerializer(Class<T> clazz, int bufferSize) {
		this(clazz, bufferSize, false);
	}

    public AnnotatedCompositeSerializer(Class<T> clazz, int bufferSize, boolean includeParentFields) {
        this.clazz      = clazz;
        this.components = new ArrayList<ComponentSerializer<?>>();
        this.bufferSize = bufferSize;

        for (Field field : getFields(clazz, includeParentFields)) {
            Component annotation = field.getAnnotation(Component.class);
            if (annotation != null) {
				Serializer s = SerializerTypeInferer.getSerializer(field.getType());
                components.add(makeComponent(field, s, annotation.ordinal()));
            }
        }

        Collections.sort(this.components);
    }

	private List<Field> getFields(Class clazz, boolean recursively) {
		List<Field> allFields = new ArrayList<Field>();
		if (clazz.getDeclaredFields() != null && clazz.getDeclaredFields().length > 0) {
			allFields.addAll(Arrays.asList(clazz.getDeclaredFields()));
		}
		if (recursively && clazz.getSuperclass() != null && !clazz.getSuperclass().equals(Object.class)) {
			allFields.addAll(getFields(clazz.getSuperclass(), true));
		}
		return allFields;
	}

    @Override
    public ByteBuffer toByteBuffer(T obj) {
        ByteBuffer bb = ByteBuffer.allocate(bufferSize);
        
        for (ComponentSerializer<?> serializer : components) {
            try {
                // First, serialize the ByteBuffer for this component
                ByteBuffer cb = serializer.serialize(obj);
                if (cb == null) {
                    cb = ByteBuffer.allocate(0);
                }

                if (cb.limit() + COMPONENT_OVERHEAD > bb.remaining()) {
                    int exponent = (int) Math.ceil(Math.log((double) (cb.limit() + COMPONENT_OVERHEAD + bb.limit())) / Math.log(2));
                    int newBufferSize = (int) Math.pow(2, exponent);
                    ByteBuffer temp = ByteBuffer.allocate(newBufferSize);
                    bb.flip();
                    temp.put(bb);
                    bb = temp;
                }
                // Write the data: <length><data><0>
                bb.putShort((short) cb.remaining());
                bb.put(cb.slice());
                bb.put(END_OF_COMPONENT);
            }
            catch (Exception e) {
				e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
        bb.flip();
        return bb;
    }

    @Override
    public T fromByteBuffer(ByteBuffer byteBuffer) {
        byteBuffer = byteBuffer.duplicate();
        try {
            T obj = createContents(clazz);
            for (ComponentSerializer<?> serializer : components) {
                ByteBuffer data = getWithShortLength(byteBuffer);
                if (data != null) {
                    if (data.remaining() > 0) {
                        serializer.deserialize(obj, data);
                    }
                    byte end_of_component = byteBuffer.get();
                    if (end_of_component != END_OF_COMPONENT) {
                        throw new RuntimeException("Invalid composite column.  Expected END_OF_COMPONENT.");
                    }
                }
                else {
                    throw new RuntimeException("Missing component data in composite type");
                }
            }
            return obj;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ComparatorType getComparatorType() {
        return ComparatorType.COMPOSITETYPE;
    }

    private static int getShortLength(ByteBuffer bb) {
        int length = (bb.get() & 0xFF) << 8;
        return length | (bb.get() & 0xFF);
    }

    private static ByteBuffer getWithShortLength(ByteBuffer bb) {
        int length = getShortLength(bb);
        return getBytes(bb, length);
    }

    private static ByteBuffer getBytes(ByteBuffer bb, int length) {
        ByteBuffer copy = bb.duplicate();
        copy.limit(copy.position() + length);
        bb.position(bb.position() + length);
        return copy;
    }

    private static <P> ComponentSerializer<P> makeComponent(Field field, Serializer<P> serializer, int ordinal) {
        return new ComponentSerializer<P>(field, serializer, ordinal);
    }

    private T createContents(Class<T> clazz) throws InstantiationException, IllegalAccessException {
        return clazz.newInstance();
    }

    public CompositeRangeBuilder buildRange() {
        return new CompositeRangeBuilder() {
            private int position = 0;

            public void nextComponent() {
                position++;
            }

            public void append(ByteBufferOutputStream out, Object value, Equality equality) {
                ComponentSerializer<?> serializer = components.get(position);
                // First, serialize the ByteBuffer for this component
                ByteBuffer cb;
                try {
                    cb = serializer.serializeValue(value);
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }

                if (cb == null) {
                    cb = EMPTY_BYTE_BUFFER;
                }

                // Write the data: <length><data><0>
                out.writeShort((short) cb.remaining());
                out.write(cb.slice());
                out.write(equality.toByte());
            }
        };
    }

    public <T1> RangeEndpoint makeEndpoint(T1 value, Equality equality) {
        RangeEndpoint endpoint = new RangeEndpoint() {
            private ByteBuffer out = ByteBuffer.allocate(bufferSize);
            private int position = 0;
            private boolean done = false;
            
            @Override
            public RangeEndpoint append(Object value, Equality equality) {
                ComponentSerializer<?> serializer = components.get(position);
                position++;
                // First, serialize the ByteBuffer for this component
                ByteBuffer cb;
                try {
                    cb = serializer.serializeValue(value);
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }

                if (cb == null) {
                    cb = EMPTY_BYTE_BUFFER;
                }

                if (cb.limit() + COMPONENT_OVERHEAD > out.remaining()) {
                    int exponent = (int) Math.ceil(Math.log((double) (cb.limit() + COMPONENT_OVERHEAD) / (double) out.limit()) / Math.log(2));
                    int newBufferSize = out.limit() * (int) Math.pow(2, exponent);
                    ByteBuffer temp = ByteBuffer.allocate(newBufferSize);
                    out.flip();
                    temp.put(out);
                    out = temp;
                }

                // Write the data: <length><data><0>
                out.putShort((short) cb.remaining());
                out.put(cb.slice());
                out.put(equality.toByte());
                return this;
            }

            @Override
            public ByteBuffer toBytes() {
                if (!done) {
                    out.flip();
                    done = true;
                }
                return out;
            }
        };
        endpoint.append(value, equality);
        return endpoint;
    }
}
