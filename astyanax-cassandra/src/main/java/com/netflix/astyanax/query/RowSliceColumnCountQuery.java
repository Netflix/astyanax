package com.netflix.astyanax.query;

import java.util.Map;

import com.netflix.astyanax.Execution;

/**
 * Interface for an operation to get the column count for a row slice or range
 * @author elandau
 *
 * @param <K>
 */
public interface RowSliceColumnCountQuery<K> extends Execution<Map<K, Integer>> {

}
