package com.netflix.astyanax.entitystore;

import java.lang.reflect.Field;
import java.util.Map;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

import junit.framework.Assert;

import org.junit.Test;

import com.netflix.astyanax.entitystore.EntityAnnotation.ColumnMapper;

public class EntityAnnotationTest {
	
	@Test
	public void basic() {
		EntityAnnotation ea = new EntityAnnotation(SampleEntity.class);
		
		Field idField = ea.getId();
		Assert.assertEquals("id", idField.getName());
		
		Map<String, ColumnMapper> cols = ea.getColumnMappers();
		System.out.println(cols);
		Assert.assertEquals(19, cols.size());
		
		Assert.assertNull(cols.get("UUID"));
		Assert.assertNotNull(cols.get("uuid"));
	}
	
	@Test(expected = IllegalArgumentException.class) 
	public void missingEntityAnnotation() {
		new EntityAnnotation(String.class);
	}

	@Entity
	private static class InvalidColumnNameEntity {
		@Id
		private String id;
		
		@Column(name="LONG.PRIMITIVE")
		private long longPrimitive;
	}
	
	@Test(expected = IllegalArgumentException.class) 
	public void invalidColumnName() {
		new EntityAnnotation(InvalidColumnNameEntity.class);
	}
}