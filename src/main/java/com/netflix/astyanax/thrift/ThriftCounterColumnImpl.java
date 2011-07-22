package com.netflix.astyanax.thrift;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;

public class ThriftCounterColumnImpl<C> implements Column<C> {

	private final C name;
	private final long value;
	
	public ThriftCounterColumnImpl(C name, long value) {
		this.name = name;
		this.value = value;
	}
	
	@Override
	public C getName() {
		return this.name;
	}

	@Override
	public <V> V getValue(Serializer<V> valSer)  {
		throw new UnsupportedOperationException(
				"CounterColumn \'" + this.name+ "\' has no generic value. Call getLongValue().");
	}

	@Override
	public String getStringValue()  {
		throw new UnsupportedOperationException(
				"CounterColumn \'" + this.name+ "\' has no String value. Call getLongValue().");
	}

	@Override
	public int getIntegerValue()  {
		throw new UnsupportedOperationException(
				"CounterColumn \'" + this.name+ "\' has no Integer value. Call getLongValue().");
	}

	@Override
	public long getLongValue()  {
		return this.value;
	}

	@Override
	public <C2> ColumnList<C2> getSubColumns(Serializer<C2> ser) {
		throw new UnsupportedOperationException(
				"CounterColumn \'" + this.name+ "\' has no sub columns. Call getLongValue().");
	}

	@Override
	public boolean isParentColumn() {
		return false;
	}

	@Override
	public byte[] getByteArrayValue() {
		throw new UnsupportedOperationException(
				"CounterColumn \'" + this.name+ "\' has no byte[] value. Call getLongValue().");
	}

	@Override
	public boolean getBooleanValue() {
		throw new UnsupportedOperationException(
				"CounterColumn \'" + this.name+ "\' has no Boolean value. Call getLongValue().");
	}

	@Override
	public ByteBuffer getByteBufferValue() {
		throw new UnsupportedOperationException(
				"CounterColumn \'" + this.name+ "\' has no ByteBuffer value. Call getLongValue().");
	}

	@Override
	public Date getDateValue() {
		throw new UnsupportedOperationException(
				"CounterColumn \'" + this.name+ "\' has no Date value. Call getLongValue().");
	}

	@Override
	public UUID getUUIDValue() {
		throw new UnsupportedOperationException(
				"CounterColumn \'" + this.name+ "\' has no UUID value. Call getLongValue().");
	}

}
