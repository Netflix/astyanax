package com.netflix.astyanax.query;

public interface IndexColumnExpression<K,C> {
	/**
	 * Set the column part of the expression
	 * @param columnName
	 * @return
	 */
	public IndexOperationExpression<K,C> whereColumn(C columnName);

}
