package com.netflix.astyanax.util;

import java.nio.ByteBuffer;

import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.Equality;
import com.netflix.astyanax.serializers.ByteBufferOutputStream;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer.ComponentSerializer;

public abstract class RangeBuilder implements ByteBufferRange {
	private ByteBufferOutputStream start = new ByteBufferOutputStream();
	private ByteBufferOutputStream end   = new ByteBufferOutputStream();
    private int limit = 100;
    private boolean reversed = false;
    private boolean lockComponent = false;
	
	abstract protected void nextComponent();
	
	abstract protected void append(ByteBufferOutputStream out, Object value, Equality equality);

	public RangeBuilder withPrefix(Object object) {
		if (lockComponent) {
			throw new IllegalStateException("Prefix cannot be added once equality has been specified");
		}
		append(start, object, Equality.EQUAL);
		append(end, object, Equality.EQUAL);
		nextComponent();
		return this;
	}

	public RangeBuilder limit(int count) {
		this.limit = count;
		return this;
	}

	public RangeBuilder reverse() {
		reversed = true;
		ByteBufferOutputStream temp = start;
		start = end;
		end = temp;
		return this;
	}
	
	public RangeBuilder greaterThan(Object value) {
		lockComponent = true;
		append(start, value, Equality.GREATER_THAN);
		return this;
	}
	
	public RangeBuilder greaterThanEquals(Object value) {
		lockComponent = true;
		append(start, value, Equality.GREATER_THAN_EQUALS);
		return this;
	}
	
	public RangeBuilder lessThan(Object value) {
		lockComponent = true;
		append(end, value, Equality.LESS_THAN);
		return this;
	}
	
	public RangeBuilder lessThanEquals(Object value) {
		lockComponent = true;
		append(end, value, Equality.LESS_THAN_EQUALS);
		return this;
	}
	
	@Override
	public ByteBuffer getStart() {
		return this.start.getByteBuffer();
	}

	@Override
	public ByteBuffer getEnd() {
		return this.end.getByteBuffer();
	}

	@Override
	public boolean isReversed() {
		return this.reversed;
	}

	@Override
	public int getLimit() {
		return this.limit;
	}
}
