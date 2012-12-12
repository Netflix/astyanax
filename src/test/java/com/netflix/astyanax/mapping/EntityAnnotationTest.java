package com.netflix.astyanax.mapping;

import java.lang.reflect.Field;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

public class EntityAnnotationTest {
	
	@Test
	public void basic() {
		EntityAnnotation ea = new EntityAnnotation(SampleEntity.class);
		
		Field idField = ea.getId();
		Assert.assertEquals("id", idField.getName());
		
		Map<String, Field> cols = ea.getColumns();
		System.out.println(cols);
		Assert.assertEquals(18, cols.size());
		
		Assert.assertNull(cols.get("UUID"));
		Assert.assertNotNull(cols.get("uuid"));
	}

}
