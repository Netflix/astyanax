package com.netflix.astyanax.query;

import com.netflix.astyanax.Execution;
import com.netflix.astyanax.model.Column;

/**
 * Interface to execute a column query on a single row.
 * @author elandau
 *
 * @param <C>
 */
public interface ColumnQuery<C> extends Execution<Column<C>>{
}
