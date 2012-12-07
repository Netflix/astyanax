package com.netflix.astyanax.index;

/**
 * Probably manage IndexContext as a singleton.
 * 
 * @author marcus
 *
 */
public class IndexCoordinationFactory {

	static IndexCoordination indexContext;
	
	static {
		indexContext = new IndexContextThreadLocalImpl();
	}
	
	public static IndexCoordination getIndexContext() {
		return indexContext;
		
	}
}
