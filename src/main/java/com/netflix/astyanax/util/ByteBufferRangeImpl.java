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
