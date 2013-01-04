package com.netflix.astyanax.entitystore;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.serializers.SerializerTypeInferer;

/**
 * utility class to map btw Entity annotations and cassandra data model
 * @param <T> entity type 
 * @param <K> rowKey type
 */
class EntityAnnotation {

	// default/package visibility
	static class ColumnMapper {
		private final Field field;
		private final Serializer<?> serializer;

		ColumnMapper(Field field, Serializer<?> serializer) {
			this.field = field;
			this.serializer = serializer;
		}

		Field getField() {
			return field;
		}

		Serializer<?> getSerializer() {
			return serializer;
		}
	}

	private final Class<?> clazz;
	private final Field idField;
	private final ImmutableMap<String, ColumnMapper> columnMappers;

	/**
	 * 
	 * @param clazz
	 * @throws IllegalArgumentException if clazz is annotated with @Entity
	 */
	EntityAnnotation(Class<?> clazz) {
		this.clazz = clazz;
		
		// clazz should be annotated with @Entity
		Entity entityAnnotation = clazz.getAnnotation(Entity.class);
		if(entityAnnotation == null)
			throw new IllegalArgumentException("entity class is NOT annotated with @Entity: " + clazz.getName());

		Field tmpIdField = null;
		ImmutableMap.Builder<String, ColumnMapper> builder = ImmutableMap.builder();
		Set<String> usedColumnNames = Sets.newHashSet();
		for (Field field : clazz.getDeclaredFields()) {
			Id idAnnotation = field.getAnnotation(Id.class);
			Column columnAnnotation = field.getAnnotation(Column.class);

			if ((idAnnotation != null) && (columnAnnotation != null)) {
				throw new IllegalArgumentException("field cannot be marked as both an Id and a Column: " + field.getName());
			}

			if(idAnnotation != null) {
				Preconditions.checkArgument(tmpIdField == null, "there are multiple fields with @Id annotation");
				field.setAccessible(true);
				tmpIdField = field;
			}

			if ((columnAnnotation != null)) {
				String columnName = getColumnName(columnAnnotation, field);
				Preconditions.checkArgument(!usedColumnNames.contains(columnName), String.format("duplicate case-insensitive column name: %s", columnName));
				usedColumnNames.add(columnName);
				field.setAccessible(true);
				// otherwise infer the Serializer from field object type
				Serializer<?> serializer = getSerializer(field);
				builder.put(columnName, new ColumnMapper(field, serializer));
			}
		}

		Preconditions.checkNotNull(tmpIdField, "there are no field with @Id annotation");
		//Preconditions.checkArgument(tmpIdField.getClass().equals(K.getClass()), String.format("@Id field type (%s) doesn't match generic type K (%s)", tmpIdField.getClass(), K.getClass()));
		idField = tmpIdField;

		this.columnMappers = builder.build();
	}

	private Serializer<?> getSerializer(Field field) {
		Serializer<?> serializer = null;
		// check if there is @Serializer annotation first
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

	private String getColumnName(Column annotation, Field field) {
		// use field name if annotation name is not set
		String name = annotation.name().isEmpty() ? field.getName() : annotation.name();
		// standardize to lower case. make column names case insensitive
		return name.toLowerCase();
	}

	Field getId() {
		return idField;
	}

	Map<String, ColumnMapper> getColumnMappers() {
		return columnMappers;
	}

	@Override
	public String toString() {
		return String.format("EntityAnnotation(%s)", clazz);
	}
}
