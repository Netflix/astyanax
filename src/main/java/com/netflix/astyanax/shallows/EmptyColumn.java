package com.netflix.astyanax.shallows;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;

public class EmptyColumn<C> implements Column<C> {

	@Override
	public C getName() {
		throw new UnsupportedOperationException();
	}

	@Override
	public <V> V getValue(Serializer<V> valSer) {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getStringValue() {
		throw new UnsupportedOperationException();
	}

	@Override
	public <C2> ColumnList<C2> getSubColumns(Serializer<C2> ser) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isParentColumn() {
		return false;
	}

	@Override
	public int getIntegerValue() {
		throw new UnsupportedOperationException();
	}

	@Override
	public long getLongValue() {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte[] getByteArrayValue() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean getBooleanValue() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ByteBuffer getByteBufferValue() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Date getDateValue() {
		throw new UnsupportedOperationException();
	}

	@Override
	public UUID getUUIDValue() {
		throw new UnsupportedOperationException();
	}
}
