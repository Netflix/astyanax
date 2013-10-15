/*******************************************************************************
 * Copyright 2011 Netflix
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.netflix.astyanax.impl;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.query.IndexOperator;
import com.netflix.astyanax.query.PreparedIndexExpression;
import com.netflix.astyanax.query.PreparedIndexValueExpression;
import com.netflix.astyanax.query.PreparedIndexOperationExpression;
import com.netflix.astyanax.serializers.BooleanSerializer;
import com.netflix.astyanax.serializers.ByteBufferSerializer;
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.serializers.DateSerializer;
import com.netflix.astyanax.serializers.DoubleSerializer;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.serializers.UUIDSerializer;

public class PreparedIndexExpressionImpl<K, C> implements PreparedIndexExpression<K, C>,
        PreparedIndexOperationExpression<K, C>, PreparedIndexValueExpression<K, C> {
    private ByteBuffer value;
    private ByteBuffer column;
    private IndexOperator operator;
    private final Serializer<C> columnSerializer;

    public PreparedIndexExpressionImpl(Serializer<C> columnSerializer) {
        this.columnSerializer = columnSerializer;
    }

    @Override
    public PreparedIndexOperationExpression<K, C> whereColumn(C columnName) {
        column = columnSerializer.toByteBuffer(columnName);
        return this;
    }

    @Override
    public ByteBuffer getColumn() {
        return column;
    }

    @Override
    public ByteBuffer getValue() {
        return value;
    }

    @Override
    public IndexOperator getOperator() {
        return operator;
    }

    @Override
    public PreparedIndexValueExpression<K, C> equals() {
        operator = IndexOperator.EQ;
        return this;
    }

    @Override
    public PreparedIndexValueExpression<K, C> greaterThan() {
        operator = IndexOperator.GT;
        return this;
    }

    @Override
    public PreparedIndexValueExpression<K, C> lessThan() {
        operator = IndexOperator.LT;
        return this;
    }

    @Override
    public PreparedIndexValueExpression<K, C> greaterThanEquals() {
        operator = IndexOperator.GTE;
        return this;
    }

    @Override
    public PreparedIndexValueExpression<K, C> lessThanEquals() {
        operator = IndexOperator.LTE;
        return this;
    }

    @Override
    public PreparedIndexExpression<K, C> value(String value) {
        this.value = StringSerializer.get().toByteBuffer(value);
        return this;
    }

    @Override
    public PreparedIndexExpression<K, C> value(long value) {
        this.value = LongSerializer.get().toByteBuffer(value);
        return this;
    }

    @Override
    public PreparedIndexExpression<K, C> value(int value) {
        this.value = IntegerSerializer.get().toByteBuffer(value);
        return this;
    }

    @Override
    public PreparedIndexExpression<K, C> value(boolean value) {
        this.value = BooleanSerializer.get().toByteBuffer(value);
        return this;
    }

    @Override
    public PreparedIndexExpression<K, C> value(Date value) {
        this.value = DateSerializer.get().toByteBuffer(value);
        return this;
    }

    @Override
    public PreparedIndexExpression<K, C> value(byte[] value) {
        this.value = BytesArraySerializer.get().toByteBuffer(value);
        return this;
    }

    @Override
    public PreparedIndexExpression<K, C> value(ByteBuffer value) {
        this.value = ByteBufferSerializer.get().toByteBuffer(value);
        return this;
    }

    @Override
    public PreparedIndexExpression<K, C> value(double value) {
        this.value = DoubleSerializer.get().toByteBuffer(value);
        return this;
    }

    @Override
    public PreparedIndexExpression<K, C> value(UUID value) {
        this.value = UUIDSerializer.get().toByteBuffer(value);
        return this;
    }

    @Override
    public <V> PreparedIndexExpression<K, C> value(V value, Serializer<V> valueSerializer) {
        this.value = valueSerializer.toByteBuffer(value);
        return this;
    }

}
