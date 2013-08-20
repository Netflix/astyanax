package com.netflix.astyanax.cql.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.cql3.CQL3Type;


public class CqlTypeMapping {

	private static Map<String, CQL3Type> directTypeMap = new HashMap<String, CQL3Type>();
	private static Map<String, CQL3Type> reverseTypeMap = new HashMap<String, CQL3Type>();
		
	static {
		
		for (CQL3Type.Native cqlType : CQL3Type.Native.values()) {
			if (!cqlType.name().contains("VAR")) {
				directTypeMap.put(cqlType.name(), cqlType);
				reverseTypeMap.put(cqlType.getType().getClass().getSimpleName(), cqlType);
				reverseTypeMap.put(cqlType.getType().getClass().getName(), cqlType);
			}
		}
	}
		
	public static String getCqlType(String typeString) {
		
		CQL3Type type = directTypeMap.get(typeString);
		type = (type == null) ? reverseTypeMap.get(typeString) : type;
			
		if (type == null) {
			throw new RuntimeException("Type not found: " + type);
		}
		return type.toString();
	}
}
