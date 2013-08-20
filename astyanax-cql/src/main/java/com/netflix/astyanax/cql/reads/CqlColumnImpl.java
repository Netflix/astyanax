package com.netflix.astyanax.cql.reads;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.datastax.driver.core.Row;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.StringSerializer;

public class CqlColumnImpl<C> implements Column<C> {

	private Row row; 
	private C columnName; 
	private int index;
	
	public CqlColumnImpl(String colName, Row row, int index) {
		this.columnName = (C) colName;
		this.row = row;
		this.index = index;
	}
	
	@Override
	public C getName() {
		return columnName;
	}

	@Override
	public ByteBuffer getRawName() {
		return StringSerializer.get().toByteBuffer(String.valueOf(columnName));
	}

	@Override
	public long getTimestamp() {
		throw new NotImplementedException();
	}

	@Override
	public <V> V getValue(Serializer<V> valSer) {
		throw new NotImplementedException();
	}

	@Override
	public String getStringValue() {
		return row.getString(index);
	}

	@Override
	public String getCompressedStringValue() {
		throw new NotImplementedException();
	}

	@Override
	public byte getByteValue() {
		return row.getBytes(index).get();
	}

	@Override
	public short getShortValue() {
		Integer i = new Integer(row.getInt(index));
		return i.shortValue();
	}

	@Override
	public int getIntegerValue() {
		return row.getInt(index);
	}

	@Override
	public float getFloatValue() {
		return row.getFloat(index);
	}

	@Override
	public double getDoubleValue() {
		return row.getDouble(index);
	}

	@Override
	public long getLongValue() {
		return row.getLong(index);
	}

	@Override
	public byte[] getByteArrayValue() {
		return row.getBytes(index).array();
	}

	@Override
	public boolean getBooleanValue() {
		return row.getBool(index);
	}

	@Override
	public ByteBuffer getByteBufferValue() {
		return row.getBytes(index);
	}

	@Override
	public Date getDateValue() {
		return row.getDate(index);
	}

	@Override
	public UUID getUUIDValue() {
		return row.getUUID(index);
	}

	@Override
	@Deprecated
	public <C2> ColumnList<C2> getSubColumns(Serializer<C2> ser) {
		throw new NotImplementedException();
	}

	@Override
	@Deprecated
	public boolean isParentColumn() {
		throw new NotImplementedException();
	}

	@Override
	public int getTtl() {
		throw new NotImplementedException();
	}

	@Override
	public boolean hasValue() {
		return !(row.isNull(index));
	}
}
