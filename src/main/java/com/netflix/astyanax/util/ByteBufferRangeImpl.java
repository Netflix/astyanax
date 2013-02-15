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
package com.netflix.astyanax.util;

import java.nio.ByteBuffer;

import com.netflix.astyanax.model.ByteBufferRange;

public class ByteBufferRangeImpl implements ByteBufferRange {
    private final ByteBuffer start;
    private final ByteBuffer end;
    private final int limit;
    private final boolean reversed;

    public ByteBufferRangeImpl(ByteBuffer start, ByteBuffer end, int limit, boolean reversed) {
        this.start = start;
        this.end = end;
        this.limit = limit;
        this.reversed = reversed;
    }

    @Override
    public ByteBuffer getStart() {
        return start;
    }

    @Override
    public ByteBuffer getEnd() {
        return end;
    }

    @Override
    public boolean isReversed() {
        return reversed;
    }

    @Override
    public int getLimit() {
        return limit;
    }

}
