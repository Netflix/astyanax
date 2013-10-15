package com.netflix.astyanax.cql.util;

import java.util.ArrayList;
import java.util.List;

import com.netflix.astyanax.KeyspaceTracerFactory;

public class ChainedContext2 {

//	private KeyspaceTracerFactory tracerFactory; 
//	
//	List<Object> contextList = new ArrayList<Object>();
//	private int index = 0; 
//	
//	public ChainedContext(KeyspaceTracerFactory tracerFactory) {
//		this.tracerFactory = tracerFactory;
//	}
//	
//	public ChainedContext() {
//		
//	}
//	
//	public ChainedContext add(Object element) {
//		contextList.add(index++, element);
//		return this;
//	}
//
//	public ChainedContext rewindForRead() {
//		index = 0;
//		return this;
//	}
//	
//	public ChainedContext skip() {
//		index++;
//		return this;
//	}
//	
//	@SuppressWarnings("unchecked")
//	public <T> T getNext(Class<T> clazz) {
//		if (index >= contextList.size()) {
//			throw new RuntimeException("Context overflow - context was not set up properly");
//		}
//		return (T)contextList.get(index++);
//	}
//	
//	public <T> T getNextNullable(Class<T> clazz) {
//		if (index >= contextList.size()) {
//			return null;
//		}
//		return (T)contextList.get(index++);
//	}
//
//	public ChainedContext clone() {
//		ChainedContext ctx = new ChainedContext(this.tracerFactory);
//		ctx.contextList.addAll(this.contextList);
//		ctx.index = contextList.size();
//		return ctx;
//	}
//	
//	public KeyspaceTracerFactory getTracerFactory() {
//		return this.tracerFactory;
//	}
//	
//	public String toString() {
//		StringBuilder sb = new StringBuilder("ChainedContext [");
//		for (Object o : contextList) {
//			sb.append(" " + o.getClass().getSimpleName());
//		}
//		
//		sb.append(" ] index currently at pos: " + index);
//		return sb.toString();
//	}
}
