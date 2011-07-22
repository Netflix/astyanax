package com.netflix.astyanax.thrift;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.BooleanSerializer;
import com.netflix.astyanax.serializers.ByteBufferSerializer;
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.serializers.DateSerializer;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.serializers.UUIDSerializer;

/**
 * 
 * 
 * TODO:  All serializers 
 * @author elandau
 *
 * @param <C>
 */
public class ThriftColumnImpl<C> implements Column<C> {
	private final byte[] value;
	private final C name;
	
	public ThriftColumnImpl(C name, byte[] value) {
		this.value = value;
		this.name = name;
	}
	
	@Override
	public C getName() {
		return name;
	}

	@Override
	public <V> V getValue(Serializer<V> valSer) {
		return valSer.fromBytes(value);
	}

	@Override
	public String getStringValue() {
		return StringSerializer.get().fromBytes(value);
	}

	@Override
	public int getIntegerValue() {
		return IntegerSerializer.get().fromBytes(value);
	}

	@Override
	public long getLongValue() {
		return LongSerializer.get().fromBytes(value);
	}
	
	@Override
	public <C2> ColumnList<C2> getSubColumns(Serializer<C2> ser) {
		
		throw new UnsupportedOperationException(
				"SimpleColumn \'" + name + "\' has no children");
	}

	@Override
	public boolean isParentColumn() {
		return false;
	}

	@Override
	public byte[] getByteArrayValue() {
		return BytesArraySerializer.get().fromBytes(value);
	}

	@Override
	public boolean getBooleanValue() {
		return BooleanSerializer.get().fromBytes(value);
	}

	@Override
	public ByteBuffer getByteBufferValue() {
		return ByteBufferSerializer.get().fromBytes(value);
	}

	@Override
	public Date getDateValue() {
		return DateSerializer.get().fromBytes(value);
	}

	@Override
	public UUID getUUIDValue() {
		return UUIDSerializer.get().fromBytes(value);
	}
}
