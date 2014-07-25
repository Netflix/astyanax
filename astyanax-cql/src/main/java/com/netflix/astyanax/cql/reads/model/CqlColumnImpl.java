package com.netflix.astyanax.cql.reads.model;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.cql.util.CqlTypeMapping;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.BooleanSerializer;
import com.netflix.astyanax.serializers.ComparatorType;
import com.netflix.astyanax.serializers.DateSerializer;
import com.netflix.astyanax.serializers.DoubleSerializer;
import com.netflix.astyanax.serializers.FloatSerializer;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.ShortSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.serializers.UUIDSerializer;

/**
 * Class that implements the {@link Column} interface. 
 * 
 * Note that since columns can be rows in CQL3, this class needs access to the java driver {@link Row}
 * within the java driver {@link ResultSet}
 * 
 * The index provided within the row indicates where to start parsing the Column data. 
 * Also this class handles reading the TTL and Timestamp on the Column as well. 
 * 
 * @author poberai
 *
 * @param <C>
 */
public class CqlColumnImpl<C> implements Column<C> {

	private Row row; 
	private C columnName; 
	private int index;
	
	private ComparatorType cType;
	
	private boolean isBlob = false;
	
	public CqlColumnImpl() {
	}
	
	public CqlColumnImpl(C colName, Row row, int index) {
		this.columnName = colName;
		this.row = row;
		this.index = index;

		Definition colDefinition  = row.getColumnDefinitions().asList().get(index);
		isBlob = colDefinition.getType() == DataType.blob();
	}
	
	public CqlColumnImpl(C colName, Row row, int index, Definition colDefinition) {
		
		this.columnName = colName;
		this.row = row;
		this.index = index;

		isBlob = colDefinition.getType() == DataType.blob();
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
		return row.getLong(index+2);
	}

	@Override
	public <V> V getValue(Serializer<V> valSer) {
		return valSer.fromByteBuffer(row.getBytes(index));
	}

	@Override
	public String getStringValue() {
		return (isBlob) ? StringSerializer.get().fromByteBuffer(row.getBytes(index)) : row.getString(index);
	}

	@Override
	public String getCompressedStringValue() {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override
	public byte getByteValue() {
		return row.getBytes(index).get();
	}

	@Override
	public short getShortValue() {
		Integer i = (isBlob) ? ShortSerializer.get().fromByteBuffer(row.getBytes(index)) : row.getInt(index);
		return i.shortValue();
	}

	@Override
	public int getIntegerValue() {
		return (isBlob) ? IntegerSerializer.get().fromByteBuffer(row.getBytes(index)) : row.getInt(index);
	}

	@Override
	public float getFloatValue() {
		return (isBlob) ? FloatSerializer.get().fromByteBuffer(row.getBytes(index)) : row.getFloat(index);
	}

	@Override
	public double getDoubleValue() {
		return (isBlob) ? DoubleSerializer.get().fromByteBuffer(row.getBytes(index)) : row.getDouble(index);
	}

	@Override
	public long getLongValue() {
		return (isBlob) ? LongSerializer.get().fromByteBuffer(row.getBytes(index)) : row.getLong(index);
	}

	@Override
	public byte[] getByteArrayValue() {
		return row.getBytes(index).array();
	}

	@Override
	public boolean getBooleanValue() {
		return (isBlob) ? BooleanSerializer.get().fromByteBuffer(row.getBytes(index)) : row.getBool(index);
	}

	@Override
	public ByteBuffer getByteBufferValue() {
		return row.getBytes(index);
	}

	@Override
	public Date getDateValue() {
		return (isBlob) ? DateSerializer.get().fromByteBuffer(row.getBytes(index)) : row.getDate(index);
	}

	@Override
	public UUID getUUIDValue() {
		return (isBlob) ? UUIDSerializer.get().fromByteBuffer(row.getBytes(index)) : row.getUUID(index);
	}

	@Override
	@Deprecated
	public <C2> ColumnList<C2> getSubColumns(Serializer<C2> ser) {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override
	@Deprecated
	public boolean isParentColumn() {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override
	public int getTtl() {
		return row.getInt(index+1);
	}

	@Override
	public boolean hasValue() {
		return (row != null) && !(row.isNull(index));
	}
	

	public Object getGenericValue() {
		ComparatorType cType = getComparatorType();
		return CqlTypeMapping.getDynamicColumn(row, cType.getSerializer(), index, null);
	}
	
	public ComparatorType getComparatorType() {
		
		if (cType != null) {
			return cType;
		}
		
		// Lazy init
		DataType type = row.getColumnDefinitions().getType(index);
		if (type.isCollection()) {
			throw new RuntimeException("This operation does not work for collection objects");
		}
		String typeString = (type.getName().name()).toUpperCase();
		cType = CqlTypeMapping.getComparatorFromCqlType(typeString);
		
		return cType;
	}
}
