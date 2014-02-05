package com.netflix.astyanax.entitystore;

import java.lang.SuppressWarnings;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.PersistenceException;

import com.google.common.base.Optional;
import com.netflix.astyanax.entitystore.Serializer;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.shallows.EmptyColumnList;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.SerializerTypeInferer;
import com.netflix.astyanax.serializers.UUIDSerializer;


import junit.framework.Assert;

import org.junit.Test;

public class EntityMapperTest {

	@Test
	public void basic() {
		EntityMapper<SampleEntity, String> entityMapper = new EntityMapper<SampleEntity, String>(SampleEntity.class, null);

		// test id field
		Field idField = entityMapper.getId();
		Assert.assertEquals("id", idField.getName());

		// test column number
		Collection<ColumnMapper> cols = entityMapper.getColumnList();
		System.out.println(cols);
		// 19 simple + 1 nested Bar
		Assert.assertEquals(24, cols.size());

		// test field without explicit column name
		// simple field name is used
		boolean foundUUID = false;
		boolean founduuid = false;
		for(ColumnMapper mapper: cols) {
			if(mapper.getColumnName().equals("UUID"))
				foundUUID = true;
			if(mapper.getColumnName().equals("uuid"))
				founduuid = true;
		}
		Assert.assertFalse(foundUUID);
		Assert.assertTrue(founduuid);
	}

	@Test(expected = IllegalArgumentException.class)
	public void missingEntityAnnotation() {
		new EntityMapper<String, String>(String.class, null);
	}

	@Entity
	private static class InvalidColumnNameEntity {
		@SuppressWarnings("unused")
		@Id
		private String id;

		@SuppressWarnings("unused")
		@Column(name="LONG.PRIMITIVE")
		private long longPrimitive;
	}

	@Test(expected = IllegalArgumentException.class)
	public void invalidColumnName() {
		new EntityMapper<InvalidColumnNameEntity, String>(InvalidColumnNameEntity.class, null);
	}

	@Test
	public void doubleIdColumnAnnotation() {
		EntityMapper<DoubleIdColumnEntity, String> entityMapper = new EntityMapper<DoubleIdColumnEntity, String>(DoubleIdColumnEntity.class, null);

		// test id field
		Field idField = entityMapper.getId();
		Assert.assertEquals("id", idField.getName());

		// test column number
		Collection<ColumnMapper> cols = entityMapper.getColumnList();
		System.out.println(cols);
		// 3 cols: id, num, str
		Assert.assertEquals(3, cols.size());
	}

    @Test
    public void customSerializerOnIdColumn() {
        EntityMapper<CustomSerializerIdEntity, UUID> entityMapper = new EntityMapper<CustomSerializerIdEntity, UUID>(CustomSerializerIdEntity.class, null);
        UUID id = UUID.randomUUID();
        ColumnList<String> columnList = new EmptyColumnList<String>();
        try {
            CustomSerializerIdEntity entity = entityMapper.constructEntity(id, columnList);
            if (!entity.getId().isPresent()) {
                Assert.fail("Optional Id is not present");
            }

            UUID theId = entity.getId().get();
            Assert.assertEquals("actual UUID values are not equal", id, theId);
        }
        catch (Exception e) {
            Assert.fail("Failed to construct becase: " + e.getMessage());
        }
    }
}
