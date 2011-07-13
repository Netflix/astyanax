package com.netflix.astyanax.query;

public interface IndexOperationExpression<K,C> {
	IndexValueExpression<K,C> equals();
	
	IndexValueExpression<K,C>  greaterThan();
	
	IndexValueExpression<K,C>  lessThan();
	
	IndexValueExpression<K,C>  greaterThanEquals();
	
	IndexValueExpression<K,C>  lessThanEquals();

}
