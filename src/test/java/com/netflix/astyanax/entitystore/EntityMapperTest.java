package com.netflix.astyanax.entitystore;

import java.lang.reflect.Field;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

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
		List<ColumnMapper> cols = entityMapper.getColumnList();
		System.out.println(cols);
		// 19 simple + 1 nested Bar
		Assert.assertEquals(20, cols.size());
		
		// test column name is normalized to lower cases
		for(ColumnMapper mapper: cols) {
			Assert.assertEquals(mapper.getColumnName().toLowerCase(), mapper.getColumnName());
		}
		
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
	
	@Test(expected = NullPointerException.class) 
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
}
