package com.netflix.astyanax.cql.util;

import java.util.ArrayList;
import java.util.List;

import com.netflix.astyanax.KeyspaceTracerFactory;

public class ChainedContext {

	private KeyspaceTracerFactory tracerFactory; 
	
	List<Object> contextList = new ArrayList<Object>();
	private int index = 0; 
	
	public ChainedContext(KeyspaceTracerFactory tracerFactory) {
		this.tracerFactory = tracerFactory;
	}
	
	public ChainedContext() {
		
	}
	
	public ChainedContext add(Object element) {
		contextList.add(index++, element);
		return this;
	}

	public ChainedContext rewindForRead() {
		index = 0;
		return this;
	}
	
	@SuppressWarnings("unchecked")
	public <T> T getNext(Class<T> clazz) {
		if (index >= contextList.size()) {
			return null;
		}
		return (T)contextList.get(index++);
	}
	
	public ChainedContext clone() {
		ChainedContext ctx = new ChainedContext(this.tracerFactory);
		ctx.contextList.addAll(this.contextList);
		ctx.index = ctx.contextList.size();
		return ctx;
	}
	
	public KeyspaceTracerFactory getTracerFactory() {
		return this.tracerFactory;
	}
}
