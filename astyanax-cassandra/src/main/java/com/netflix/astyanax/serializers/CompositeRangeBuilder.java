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
package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;

import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.Equality;

public abstract class CompositeRangeBuilder implements ByteBufferRange {
    private ByteBufferOutputStream start = new ByteBufferOutputStream();
    private ByteBufferOutputStream end = new ByteBufferOutputStream();
    private int limit = Integer.MAX_VALUE;
    private boolean reversed = false;
    private boolean lockComponent = false;

    abstract protected void nextComponent();

    abstract protected void append(ByteBufferOutputStream out, Object value, Equality equality);

    public CompositeRangeBuilder withPrefix(Object object) {
        if (lockComponent) {
            throw new IllegalStateException("Prefix cannot be added once equality has been specified");
        }
        append(start, object, Equality.EQUAL);
        append(end, object, Equality.EQUAL);
        nextComponent();
        return this;
    }

    public CompositeRangeBuilder limit(int count) {
        this.limit = count;
        return this;
    }

    public CompositeRangeBuilder reverse() {
        reversed = true;
        ByteBufferOutputStream temp = start;
        start = end;
        end = temp;
        return this;
    }

    public CompositeRangeBuilder greaterThan(Object value) {
        lockComponent = true;
        append(start, value, Equality.GREATER_THAN);
        return this;
    }

    public CompositeRangeBuilder greaterThanEquals(Object value) {
        lockComponent = true;
        append(start, value, Equality.GREATER_THAN_EQUALS);
        return this;
    }

    public CompositeRangeBuilder lessThan(Object value) {
        lockComponent = true;
        append(end, value, Equality.LESS_THAN);
        return this;
    }

    public CompositeRangeBuilder lessThanEquals(Object value) {
        lockComponent = true;
        append(end, value, Equality.LESS_THAN_EQUALS);
        return this;
    }

    @Override
    @Deprecated
    public ByteBuffer getStart() {
        return start.getByteBuffer();
    }

    @Override
    @Deprecated
    public ByteBuffer getEnd() {
        return end.getByteBuffer();
    }

    @Override
    @Deprecated
    public boolean isReversed() {
        return reversed;
    }

    @Override
    @Deprecated
    public int getLimit() {
        return limit;
    }

    public ByteBufferRange build() {
        return new ByteBufferRange() {
            @Override
            public ByteBuffer getStart() {
                return start.getByteBuffer();
            }

            @Override
            public ByteBuffer getEnd() {
                return end.getByteBuffer();
            }

            @Override
            public boolean isReversed() {
                return reversed;
            }

            @Override
            public int getLimit() {
                return limit;
            }
        };
    }
}
