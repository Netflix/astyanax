package com.netflix.astyanax.model;

import java.nio.ByteBuffer;
import java.util.List;

public class Composite extends AbstractComposite {

    public Composite() {
        super(false);
    }

    public Composite(Object... o) {
        super(false, o);
    }

    public Composite(List<?> l) {
        super(false, l);
    }

    public static Composite fromByteBuffer(ByteBuffer byteBuffer) {

        Composite composite = new Composite();
        composite.deserialize(byteBuffer);

        return composite;
    }

    public static ByteBuffer toByteBuffer(Object... o) {
        Composite composite = new Composite(o);
        return composite.serialize();
    }

    public static ByteBuffer toByteBuffer(List<?> l) {
        Composite composite = new Composite(l);
        return composite.serialize();
    }
}
