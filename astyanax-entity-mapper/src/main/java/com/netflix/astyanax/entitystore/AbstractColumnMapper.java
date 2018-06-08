/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
