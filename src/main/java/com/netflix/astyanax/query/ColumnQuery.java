package com.netflix.astyanax.query;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;

/**
 * Interface to execute a column query on a single row.
 * @author elandau
 *
 * @param <C>
 */
public interface ColumnQuery<C> {
	OperationResult<Column<C>> execute() throws ConnectionException;
}
