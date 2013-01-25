package com.netflix.astyanax.entitystore;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.model.ColumnList;

interface ColumnMapper {
	
	public String getColumnName();

	/**
	 * @return true if set, false if skipped due to null value for nullable field
	 * @throws IllegalArgumentException if value is null and field is NOT nullable
	 */
	public boolean fillMutationBatch(Object entity, ColumnListMutation<String> clm) throws Exception;
	
	/**
	 * @return true if set, false if skipped due to non-existent column for nullable field
	 * @throws IllegalArgumentException if value is null and field is NOT nullable
	 */
	public boolean setField(Object entity, ColumnList<String> cl) throws Exception;
}
