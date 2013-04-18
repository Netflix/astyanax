package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;

import com.netflix.astyanax.model.DynamicComposite;

/**
 * @author Todd Nine
 * 
 */
public class DynamicCompositeSerializer extends
        AbstractSerializer<DynamicComposite> {
    private static final DynamicCompositeSerializer instance = new DynamicCompositeSerializer();

    public static DynamicCompositeSerializer get() {
        return instance;
    }

    @Override
    public ByteBuffer toByteBuffer(DynamicComposite obj) {
        return obj.serialize();
    }

    @Override
    public DynamicComposite fromByteBuffer(ByteBuffer byteBuffer) {
        if (byteBuffer == null)
            return null;
        ByteBuffer dup = byteBuffer.duplicate();
        DynamicComposite composite = new DynamicComposite();
        composite.deserialize(dup);
        return composite;
    }

    @Override
    public ComparatorType getComparatorType() {
        return ComparatorType.DYNAMICCOMPOSITETYPE;
    }

    @Override
    public ByteBuffer fromString(String string) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getString(ByteBuffer byteBuffer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer getNext(ByteBuffer byteBuffer) {
        throw new IllegalStateException(
                "DynamicComposite columns can't be paginated this way.");
    }
}
