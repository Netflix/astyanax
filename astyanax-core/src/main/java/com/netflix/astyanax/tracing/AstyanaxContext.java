package com.netflix.astyanax.tracing;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AstyanaxContext {
	
	private final Map<Object, Object> context;

	public AstyanaxContext() {
		this(new ConcurrentHashMap<Object, Object>());
	}

	public AstyanaxContext(Map<Object, Object> context) {
		super();
		this.context = context;
	}

    public Object get(Object name) {
        return context.get(name);
    }
    
    public Object set(Object name, Object value) {
    	return context.put(name, value);
    }	
}