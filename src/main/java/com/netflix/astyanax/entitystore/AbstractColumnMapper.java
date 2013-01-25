package com.netflix.astyanax.entitystore;

import java.lang.reflect.Field;

import javax.persistence.Column;

import com.google.common.base.Strings;


public abstract class AbstractColumnMapper implements ColumnMapper {

	protected String deriveColumnName(Column annotation, Field field, String prefix) {
		// use field name if annotation name is not set
		String name = annotation.name().isEmpty() ? field.getName() : annotation.name();
		
		// dot is a reserved char as separator
		if(name.indexOf(".") >= 0)
			throw new IllegalArgumentException("illegal column name containing reserved dot (.) char: " + name);
		
		// prepend prefix if not empty with dot separator
		if(!Strings.isNullOrEmpty(prefix))
			name = prefix + "." + name;
		
		return name;
	}

}
