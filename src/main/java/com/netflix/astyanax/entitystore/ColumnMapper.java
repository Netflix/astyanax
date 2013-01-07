package com.netflix.astyanax.entitystore;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.model.ColumnList;

interface ColumnMapper {
	
	public String getColumnName();

	public void fillMutationBatch(Object entity, ColumnListMutation<String> clm) throws Exception;
	
	public void setField(Object entity, ColumnList<String> cl) throws Exception;
}
