package com.netflix.astyanax.serializers;

public class UnknownComparatorException extends Exception {
    private static final long serialVersionUID = -208259105321284073L;

    public UnknownComparatorException(String type) {
        super(type);
    }
}
