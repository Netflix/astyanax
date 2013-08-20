package com.netflix.astyanax.cql.util;

import java.util.ArrayList;
import java.util.List;

public class ChainedContext {

	List<Object> contextList = new ArrayList<Object>();
	private int index = 0; 
	
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
		return (T)contextList.get(index++);
	}
	
	public ChainedContext clone() {
		ChainedContext ctx = new ChainedContext();
		ctx.contextList.addAll(this.contextList);
		ctx.index = ctx.contextList.size();
		return ctx;
	}
}
