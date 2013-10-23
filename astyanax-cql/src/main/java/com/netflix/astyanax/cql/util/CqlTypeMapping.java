package com.netflix.astyanax.cql.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.cassandra.cql3.CQL3Type;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.datastax.driver.core.Row;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.cql.schema.CqlColumnFamilyDefinitionImpl;
import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer.ComponentSerializer;
import com.netflix.astyanax.serializers.ComparatorType;


public class CqlTypeMapping {

	private static Map<String, CQL3Type> directTypeMap = new HashMap<String, CQL3Type>();
	private static Map<String, CQL3Type> reverseTypeMap = new HashMap<String, CQL3Type>();
	
	private static Map<CQL3Type, ComparatorType> cql3ToComparatorTypeMap = new HashMap<CQL3Type, ComparatorType>();
	private static Map<String, CQL3Type> reverseCql3ToComparatorTypeMap = new HashMap<String, CQL3Type>();
		
	static {
		
		for (CQL3Type.Native cqlType : CQL3Type.Native.values()) {
			if (!cqlType.name().contains("VAR")) {
				directTypeMap.put(cqlType.name().toLowerCase(), cqlType);
				reverseTypeMap.put(cqlType.getType().getClass().getSimpleName(), cqlType);
				reverseTypeMap.put(cqlType.getType().getClass().getName(), cqlType);
			}
		}
		
		cql3ToComparatorTypeMap.put(CQL3Type.Native.ASCII,     ComparatorType.ASCIITYPE);
		cql3ToComparatorTypeMap.put(CQL3Type.Native.BIGINT,    ComparatorType.INTEGERTYPE);
		cql3ToComparatorTypeMap.put(CQL3Type.Native.BLOB,      ComparatorType.BYTESTYPE);    
		cql3ToComparatorTypeMap.put(CQL3Type.Native.BOOLEAN,   ComparatorType.BOOLEANTYPE);  
		cql3ToComparatorTypeMap.put(CQL3Type.Native.COUNTER,   ComparatorType.COUNTERTYPE); 
		cql3ToComparatorTypeMap.put(CQL3Type.Native.DECIMAL,   ComparatorType.DECIMALTYPE);
		cql3ToComparatorTypeMap.put(CQL3Type.Native.DOUBLE,    ComparatorType.DOUBLETYPE);   
		cql3ToComparatorTypeMap.put(CQL3Type.Native.FLOAT,     ComparatorType.FLOATTYPE);   
		//cql3ToComparatorTypeMap.put(CQL3Type.Native.INET,      null);    
		cql3ToComparatorTypeMap.put(CQL3Type.Native.INT,       ComparatorType.INT32TYPE);
		cql3ToComparatorTypeMap.put(CQL3Type.Native.TEXT,      ComparatorType.UTF8TYPE);   
		cql3ToComparatorTypeMap.put(CQL3Type.Native.TIMESTAMP, ComparatorType.DATETYPE);
		cql3ToComparatorTypeMap.put(CQL3Type.Native.UUID,      ComparatorType.UUIDTYPE);     
		cql3ToComparatorTypeMap.put(CQL3Type.Native.VARCHAR,   ComparatorType.UTF8TYPE); 
		cql3ToComparatorTypeMap.put(CQL3Type.Native.VARINT,    ComparatorType.INTEGERTYPE);   
		cql3ToComparatorTypeMap.put(CQL3Type.Native.TIMEUUID,  ComparatorType.TIMEUUIDTYPE); 
		
		for (Entry<CQL3Type, ComparatorType> e : cql3ToComparatorTypeMap.entrySet()) {
			CQL3Type cql3Type = e.getKey();
			ComparatorType compType = e.getValue();
			reverseCql3ToComparatorTypeMap.put(compType.getClassName(), cql3Type);
			reverseCql3ToComparatorTypeMap.put(compType.getTypeName(), cql3Type);
		}
	}
		
	public static String getCqlType(String typeString) {
		
		CQL3Type type = directTypeMap.get(typeString.toLowerCase());
		type = (type == null) ? reverseTypeMap.get(typeString) : type;
		type = (type == null) ? reverseCql3ToComparatorTypeMap.get(typeString) : type;
			
		if (type == null) {
			throw new RuntimeException("Type not found: " + type);
		}
		return type.toString();
	}
	
	public static ComparatorType getComparatorType(CQL3Type cqlType) {
		return cql3ToComparatorTypeMap.get(cqlType);
	}
	
//	public static <T> Object getDynamicColumn(Row row, Serializer<T> serializer) {
//		int numCols = row.getColumnDefinitions().size();
//		if (numCols < 2) {
//			throw new RuntimeException("Not enough columns for in row for parsing the column name");
//		}
//		return getDynamicColumn(row, serializer, numCols-2);
//	}
	
	private static <T> Object getDynamicColumn(Row row, Serializer<T> serializer, String columnName, ColumnFamily<?,?> cf) {
		
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
			return getCompositeColumn(row, (AnnotatedCompositeSerializer<?>) serializer, cf);
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
	
	public static <T> Object getDynamicColumn(Row row, Serializer<T> serializer, int columnIndex, ColumnFamily<?,?> cf) {
		
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
			return getCompositeColumn(row, (AnnotatedCompositeSerializer<?>) serializer, cf);
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
	
	
	private static Object getCompositeColumn(Row row, AnnotatedCompositeSerializer<?> compositeSerializer, ColumnFamily<?,?> cf) {
		
		Class<?> clazz = compositeSerializer.getClazz();
		
		Object obj = null;
		try {
			obj = clazz.newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		CqlColumnFamilyDefinitionImpl cfDef = (CqlColumnFamilyDefinitionImpl) cf.getColumnFamilyDefinition();
		List<ColumnDefinition> pkList = cfDef.getPartitionKeyColumnDefinitionList();
		
		int componentIndex = 1;
		for (ComponentSerializer<?> component : compositeSerializer.getComponents()) {
			
			Object value = getDynamicColumn(row, component.getSerializer(), pkList.get(componentIndex).getName(), cf);
			try {
				//System.out.println("Value: " + value);
				component.setFieldValueDirectly(obj, value);
				componentIndex++;
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		return obj;
	}
}
