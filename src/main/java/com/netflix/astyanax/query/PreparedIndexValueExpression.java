package com.netflix.astyanax.query;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

import com.netflix.astyanax.Serializer;

public interface PreparedIndexValueExpression<K, C> {

    PreparedIndexExpression<K, C> value(String value);

    PreparedIndexExpression<K, C> value(long value);

    PreparedIndexExpression<K, C> value(int value);

    PreparedIndexExpression<K, C> value(boolean value);

    PreparedIndexExpression<K, C> value(Date value);

    PreparedIndexExpression<K, C> value(byte[] value);

    PreparedIndexExpression<K, C> value(ByteBuffer value);

    PreparedIndexExpression<K, C> value(double value);

    PreparedIndexExpression<K, C> value(UUID value);

    <V> PreparedIndexExpression<K, C> value(V value, Serializer<V> valueSerializer);

}
