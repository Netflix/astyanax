package com.netflix.astyanax.entitystore;

import java.lang.reflect.Field;

import javax.persistence.Column;

public abstract class AbstractColumnMapper implements ColumnMapper {
    protected final Field              field;
    protected final Column             columnAnnotation;
    protected final String             columnName;
    
    public AbstractColumnMapper(Field field) {
        this.field            = field;
        this.columnAnnotation = field.getAnnotation(Column.class);
        
		// use field name if annotation name is not set
		String name = columnAnnotation.name().isEmpty() ? field.getName() : columnAnnotation.name();
		
		// dot is a reserved char as separator
		if(name.indexOf(".") >= 0)
			throw new IllegalArgumentException("illegal column name containing reserved dot (.) char: " + name);
		
        this.columnName       = name;
	}

    public Field getField() {
        return this.field;
    }
}
