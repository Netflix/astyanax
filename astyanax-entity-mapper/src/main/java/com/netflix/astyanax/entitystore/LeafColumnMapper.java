/*******************************************************************************
 * Copyright 2011 Netflix
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.netflix.astyanax.entitystore;

import java.lang.reflect.Field;
import java.util.Iterator;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Serializer;

class LeafColumnMapper extends AbstractColumnMapper {
	
	private final Serializer<?> serializer;

	LeafColumnMapper(final Field field) {
	    super(field);
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
	public boolean fillMutationBatch(Object entity, ColumnListMutation<String> clm, String prefix) throws Exception {
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
		clm.putColumn(prefix + columnName, value, valueSerializer, null);
		return true;
	}
	
    @Override
    public boolean setField(Object entity, Iterator<String> name, com.netflix.astyanax.model.Column<String> column) throws Exception {
        if (name.hasNext()) 
            return false;
        final Object fieldValue = column.getValue(serializer);
        field.set(entity, fieldValue);
        return true;
    }

    @Override
    public void validate(Object entity) throws Exception {
        if (field.get(entity) == null && !columnAnnotation.nullable())
            throw new IllegalArgumentException("cannot find non-nullable column: " + columnName);
    }
}
