package com.netflix.astyanax.index;


public interface IndexContext {

	
	<C,V> void addIndexRead(IndexMapping<C,V> mapping);
	
	<C,V> void modifyIndexValue(C key, V newValue) throws NoReadException;
	
	
	
	public class NoReadException extends Exception {
		
	}
}
