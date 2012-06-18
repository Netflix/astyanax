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
package com.netflix.astyanax;


import com.netflix.astyanax.serializers.*;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

/**
 * Abstract implementation of a row mutation
 *
 * @author lucky
 *
 * @param <C>
 */
public abstract class AbstractColumnListMutation<C> implements ColumnListMutation<C> {
    protected long timestamp;
    protected Integer defaultTtl = null;

    @Override
    public ColumnListMutation<C> putColumn(C columnName, String value, Integer ttl) {
        return putColumn(columnName, value, StringSerializer.get(), ttl);
    }

    @Override
    public ColumnListMutation<C> putColumn(final C columnName, final String value) {
        return putColumn(columnName, value, null);
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, byte[] value, Integer ttl) {
        return putColumn(columnName, value, BytesArraySerializer.get(), ttl);
    }

    @Override
    public ColumnListMutation<C> putColumn(final C columnName, final byte[] value) {
        return putColumn(columnName, value, null);
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, int value, Integer ttl) {
        return putColumn(columnName, value, IntegerSerializer.get(), ttl);
    }

    @Override
    public ColumnListMutation<C> putColumn(final C columnName, final int value) {
        return putColumn(columnName, value, null);
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, long value, Integer ttl) {
        return putColumn(columnName, value, LongSerializer.get(), ttl);
    }

    @Override
    public ColumnListMutation<C> putColumn(final C columnName, final long value) {
        return putColumn(columnName, value, null);
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, boolean value, Integer ttl) {
        return putColumn(columnName, value, BooleanSerializer.get(), ttl);
    }

    @Override
    public ColumnListMutation<C> putColumn(final C columnName, final boolean value) {
        return putColumn(columnName, value, null);
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, ByteBuffer value, Integer ttl) {
        return putColumn(columnName, value, ByteBufferSerializer.get(), ttl);
    }

    @Override
    public ColumnListMutation<C> putColumn(final C columnName, final ByteBuffer value) {
        return putColumn(columnName, value, null);
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, Date value, Integer ttl) {
        return putColumn(columnName, value, DateSerializer.get(), ttl);
    }

    @Override
    public ColumnListMutation<C> putColumn(final C columnName, final Date value) {
        return putColumn(columnName, value, null);
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, float value, Integer ttl) {
        return putColumn(columnName, value, FloatSerializer.get(), ttl);
    }

    @Override
    public ColumnListMutation<C> putColumn(final C columnName, final float value) {
        return putColumn(columnName, value, null);
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, double value, Integer ttl) {
        return putColumn(columnName, value, DoubleSerializer.get(), ttl);
    }

    @Override
    public ColumnListMutation<C> putColumn(final C columnName, final double value) {
        return putColumn(columnName, value, null);
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, UUID value, Integer ttl) {
        return putColumn(columnName, value, UUIDSerializer.get(), ttl);
    }

    @Override
    public ColumnListMutation<C> putColumn(final C columnName, final UUID value) {
        return putColumn(columnName, value, null);
    }

    @Override
    public ColumnListMutation<C> putEmptyColumn(final C columnName) {
        return putEmptyColumn(columnName, null);
    }

    @Override
    public ColumnListMutation<C> setTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    @Override
    public ColumnListMutation<C> setDefaultTtl(Integer ttl) {
        this.defaultTtl = ttl;
        return this;
    }
}
