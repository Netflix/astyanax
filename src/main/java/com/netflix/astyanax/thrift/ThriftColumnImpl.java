package com.netflix.astyanax.thrift;

import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

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
}
