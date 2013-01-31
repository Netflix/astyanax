package com.netflix.astyanax.entitystore;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import javax.persistence.Column;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.SerializerTypeInferer;

class LeafColumnMapper extends AbstractColumnMapper {
	
	private final Field field;
	private final Column columnAnnotation;
	private final String columnName;
	private final Serializer<?> serializer;

	LeafColumnMapper(final Field field, final String prefix) {
		this.field = field;
		this.columnAnnotation = field.getAnnotation(Column.class);
		this.columnName = deriveColumnName(columnAnnotation, field, prefix);
		this.serializer = MappingUtils.getSerializerForField(field);
	}

	@Override
	public String getColumnName() {
		return columnName;
	}
	
	Serializer<?> getSerializer() {
	    return serializer;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public boolean fillMutationBatch(Object entity, ColumnListMutation<String> clm) throws Exception {
		Object value = field.get(entity);
		if(value == null) {
			if(columnAnnotation.nullable())
				return false; // skip
			else
				throw new IllegalArgumentException("cannot write non-nullable column with null value: " + columnName);
		}
		@SuppressWarnings("rawtypes")
		final Serializer valueSerializer = serializer;
		// TODO: suppress the unchecked raw type now.
		// we have to use the raw type to avoid compiling error
		clm.putColumn(columnName, value, valueSerializer, null);
		return true;
	}
	
	@Override
	public boolean setField(Object entity, ColumnList<String> cl) throws Exception {
		final com.netflix.astyanax.model.Column<String> c = cl.getColumnByName(columnName);
		if(c == null) {
			if(columnAnnotation.nullable())
				return false;
			else
				throw new IllegalArgumentException("cannot find non-nullable column: " + columnName);
		}
		final Object fieldValue = c.getValue(serializer);
		field.set(entity, fieldValue);
		return true;
	}
}
