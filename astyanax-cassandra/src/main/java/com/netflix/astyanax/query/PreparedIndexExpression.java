package com.netflix.astyanax.query;

import java.nio.ByteBuffer;

public interface PreparedIndexExpression<K, C> extends PreparedIndexColumnExpression<K, C> {
    public ByteBuffer getColumn();

    public ByteBuffer getValue();

    public IndexOperator getOperator();
}
