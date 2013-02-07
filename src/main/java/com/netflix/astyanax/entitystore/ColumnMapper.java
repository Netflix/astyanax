package com.netflix.astyanax.entitystore;

import java.lang.reflect.Field;
import java.util.Iterator;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.model.Column;

interface ColumnMapper {
	
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
