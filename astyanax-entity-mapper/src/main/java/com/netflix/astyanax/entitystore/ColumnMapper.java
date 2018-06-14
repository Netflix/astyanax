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
import java.util.Iterator;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.model.Column;

public interface ColumnMapper {
	
	public String getColumnName();

	/**
	 * @return true if set, false if skipped due to null value for nullable field
	 * @throws IllegalArgumentException if value is null and field is NOT nullable
	 */
	public boolean fillMutationBatch(Object entity, ColumnListMutation<String> clm, String prefix) throws Exception;
	
	/**
	 * @return true if set, false if skipped due to non-existent column for nullable field
	 * @throws IllegalArgumentException if value is null and field is NOT nullable
	 */
	public boolean setField(Object entity, Iterator<String> name, Column<String> column) throws Exception;
	
	/**
	 * Perform a validation step either before persisting or after loading 
	 * @throws Exception
	 */
	public void validate(Object entity) throws Exception;
	
	/**
	 * Return the field associated with this mapper
	 */
	public Field getField();
}
