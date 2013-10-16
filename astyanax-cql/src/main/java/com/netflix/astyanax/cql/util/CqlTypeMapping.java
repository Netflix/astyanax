package com.netflix.astyanax.cql.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.cql3.CQL3Type;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.datastax.driver.core.Row;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer.ComponentSerializer;
import com.netflix.astyanax.serializers.ComparatorType;


public class CqlTypeMapping {

	private static Map<String, CQL3Type> directTypeMap = new HashMap<String, CQL3Type>();
	private static Map<String, CQL3Type> reverseTypeMap = new HashMap<String, CQL3Type>();
		
	static {
		
		for (CQL3Type.Native cqlType : CQL3Type.Native.values()) {
			if (!cqlType.name().contains("VAR")) {
				directTypeMap.put(cqlType.name().toLowerCase(), cqlType);
				reverseTypeMap.put(cqlType.getType().getClass().getSimpleName(), cqlType);
				reverseTypeMap.put(cqlType.getType().getClass().getName(), cqlType);
			}
		}
	}
		
	public static String getCqlType(String typeString) {
		
		CQL3Type type = directTypeMap.get(typeString.toLowerCase());
		type = (type == null) ? reverseTypeMap.get(typeString) : type;
			
		if (type == null) {
			throw new RuntimeException("Type not found: " + type);
		}
		return type.toString();
	}
	
	
	public static <T> Object getDynamicColumn(Row row, Serializer<T> serializer) {
		int numCols = row.getColumnDefinitions().size();
		if (numCols < 2) {
			throw new RuntimeException("Not enough columns for in row for parsing the column name");
		}
		return getDynamicColumn(row, serializer, numCols-2);
	}
	
	private static <T> Object getDynamicColumn(Row row, Serializer<T> serializer, String columnName) {
		
		ComparatorType comparatorType = serializer.getComparatorType();
		
		switch(comparatorType) {

		case ASCIITYPE:
			return row.getString(columnName);
		case BYTESTYPE:
			return row.getBytes(columnName);
		case INTEGERTYPE:
			return row.getInt(columnName);
		case INT32TYPE:
			return row.getInt(columnName);
		case DECIMALTYPE:
			return row.getFloat(columnName);
		case LEXICALUUIDTYPE:
			return row.getUUID(columnName);
		case LOCALBYPARTITIONERTYPE:
		    return row.getBytes(columnName);
		case LONGTYPE:
		    return row.getLong(columnName);
		case TIMEUUIDTYPE:
		    return row.getUUID(columnName);
		case UTF8TYPE:
		    return row.getString(columnName);
		case COMPOSITETYPE:
			return getCompositeColumn(row, (AnnotatedCompositeSerializer<?>) serializer);
		case DYNAMICCOMPOSITETYPE:
			throw new NotImplementedException();
		case UUIDTYPE:
		    return row.getUUID(columnName);
		case COUNTERTYPE:
		    return row.getLong(columnName);
		case DOUBLETYPE:
		    return row.getDouble(columnName);
		case FLOATTYPE:
		    return row.getFloat(columnName);
		case BOOLEANTYPE:
		    return row.getBool(columnName);
		case DATETYPE:
		    return row.getDate(columnName);
		    
		default:
			throw new RuntimeException("Could not recognize comparator type: " + comparatorType.getTypeName());
		}
	}
	
	public static <T> Object getDynamicColumn(Row row, Serializer<T> serializer, int columnIndex) {
		
		ComparatorType comparatorType = serializer.getComparatorType();
		
		switch(comparatorType) {

		case ASCIITYPE:
			return row.getString(columnIndex);
		case BYTESTYPE:
			return row.getBytes(columnIndex);
		case INTEGERTYPE:
			return row.getInt(columnIndex);
		case INT32TYPE:
			return row.getInt(columnIndex);
		case DECIMALTYPE:
			return row.getFloat(columnIndex);
		case LEXICALUUIDTYPE:
			return row.getUUID(columnIndex);
		case LOCALBYPARTITIONERTYPE:
		    return row.getBytes(columnIndex);
		case LONGTYPE:
		    return row.getLong(columnIndex);
		case TIMEUUIDTYPE:
		    return row.getUUID(columnIndex);
		case UTF8TYPE:
		    return row.getString(columnIndex);
		case COMPOSITETYPE:
			return getCompositeColumn(row, (AnnotatedCompositeSerializer<?>) serializer);
		case DYNAMICCOMPOSITETYPE:
			throw new NotImplementedException();
		case UUIDTYPE:
		    return row.getUUID(columnIndex);
		case COUNTERTYPE:
		    return row.getLong(columnIndex);
		case DOUBLETYPE:
		    return row.getDouble(columnIndex);
		case FLOATTYPE:
		    return row.getFloat(columnIndex);
		case BOOLEANTYPE:
		    return row.getBool(columnIndex);
		case DATETYPE:
		    return row.getDate(columnIndex);
		    
		default:
			throw new RuntimeException("Could not recognize comparator type: " + comparatorType.getTypeName());
		}
	}
	
	
	private static Object getCompositeColumn(Row row, AnnotatedCompositeSerializer<?> compositeSerializer) {
		
		Class<?> clazz = compositeSerializer.getClazz();
		
		Object obj = null;
		try {
			obj = clazz.newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		int columnIndex = 1;
		
		for (ComponentSerializer<?> component : compositeSerializer.getComponents()) {
			
			Object value = getDynamicColumn(row, component.getSerializer(), "column" + columnIndex++);
			try {
				System.out.println("Value: " + value);
				component.setFieldValueDirectly(obj, value);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		return obj;
	}
}
