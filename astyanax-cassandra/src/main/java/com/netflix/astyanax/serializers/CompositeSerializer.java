package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;
import java.util.List;

import com.netflix.astyanax.model.Composite;

public class CompositeSerializer extends AbstractSerializer<Composite> {

    private static final CompositeSerializer instance = new CompositeSerializer();

    public static CompositeSerializer get() {
        return instance;
    }

    @Override
    public ByteBuffer toByteBuffer(Composite obj) {
        return obj.serialize();
    }

    @Override
    public Composite fromByteBuffer(ByteBuffer byteBuffer) {
        Composite composite = new Composite();
        composite.setComparatorsByPosition(getComparators());
        composite.deserialize(byteBuffer);
        return composite;
    }

    @Override
    public ByteBuffer getNext(ByteBuffer byteBuffer) {
        throw new IllegalStateException(
                "Composite columns can't be paginated this way.  Use SpecificCompositeSerializer instead.");
    }

    @Override
    public ComparatorType getComparatorType() {
        return ComparatorType.COMPOSITETYPE;
    }

    public List<String> getComparators() {
        return null;
    }
}
