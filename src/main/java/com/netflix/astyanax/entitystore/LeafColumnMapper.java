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
		this.serializer = getSerializer(field);
	}

	private Serializer<?> getSerializer(Field field) {
		Serializer<?> serializer = null;
		// check if there is explicit @Serializer annotation first
		com.netflix.astyanax.entitystore.Serializer serializerAnnotation = field.getAnnotation(com.netflix.astyanax.entitystore.Serializer.class);
		if(serializerAnnotation != null) {
			final Class<?> serializerClazz = serializerAnnotation.value();
			// check type
			if(!(Serializer.class.isAssignableFrom(serializerClazz)))
				throw new RuntimeException("annotated serializer class is not a subclass of com.netflix.astyanax.Serializer");
			// invoke public static get() method
			try {
				Method getInstanceMethod = serializerClazz.getMethod("get");
				serializer = (Serializer<?>) getInstanceMethod.invoke(null);
			} catch(Exception e) {
				throw new RuntimeException("Failed to get or invoke public static get() method", e);
			}
		} else {
			// otherwise automatically infer the Serializer type from field object type
			serializer = SerializerTypeInferer.getSerializer(field.getType());
		}
		return serializer;
	}

	@Override
	public String getColumnName() {
		return columnName;
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
		final Integer ttl = getTTL(field);
		// TODO: suppress the unchecked raw type now.
		// we have to use the raw type to avoid compiling error
		clm.putColumn(columnName, value, valueSerializer, ttl);
		return true;
	}
	
	private Integer getTTL(Field field) {
		// if there is no field level @TTL annotation, return null.
		// then the default global value (which could also be valid null).
		TTL ttlAnnotation = field.getAnnotation(TTL.class);
		if(ttlAnnotation != null) {
			return ttlAnnotation.value();
		} else {
			return null;
		}
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
