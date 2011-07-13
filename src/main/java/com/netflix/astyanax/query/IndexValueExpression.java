package com.netflix.astyanax.query;

public interface IndexValueExpression<K,C>  {
	
	public IndexQuery<K,C> value(String value);
	
	public IndexQuery<K,C> value(long value);

	public IndexQuery<K,C> value(int value);

	public IndexQuery<K,C> value(boolean value);
}
