package com.netflix.astyanax.cql.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;

import com.datastax.driver.core.Row;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.cql.schema.CqlColumnFamilyDefinitionImpl;
import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer.ComponentSerializer;
import com.netflix.astyanax.serializers.ComparatorType;

/**
 * Helpful utility that maps the different data types and helps translate to and from Astyanax and java driver objects.
 * 
 * @author poberai
 *
 */
public class CqlTypeMapping {

	private static Map<String, String> comparatorToCql3Type = new HashMap<String, String>();
	private static Map<String, ComparatorType> cqlToComparatorType = new HashMap<String, ComparatorType>();
		
	static {
		initComparatorTypeMap();
	}
		
	private static void initComparatorTypeMap() {
		
		Map<ComparatorType, String> tmpMap = new HashMap<ComparatorType, String>();

		tmpMap.put(ComparatorType.ASCIITYPE, "ASCII");
		tmpMap.put(ComparatorType.BYTESTYPE, "BLOB");    
		tmpMap.put(ComparatorType.BOOLEANTYPE, "BOOLEAN");  
		tmpMap.put(ComparatorType.COUNTERTYPE, "COUNTER"); 
		tmpMap.put(ComparatorType.DECIMALTYPE, "DECIMAL");
		tmpMap.put(ComparatorType.DOUBLETYPE, "DOUBLE");   
		tmpMap.put(ComparatorType.FLOATTYPE, "FLOAT");   
		tmpMap.put(ComparatorType.LONGTYPE, "BIGINT");
		tmpMap.put(ComparatorType.INT32TYPE, "INT");
		tmpMap.put(ComparatorType.UTF8TYPE, "TEXT");   
		tmpMap.put(ComparatorType.DATETYPE, "TIMESTAMP");
		tmpMap.put(ComparatorType.UUIDTYPE, "UUID");     
		tmpMap.put(ComparatorType.INTEGERTYPE, "VARINT");   
		tmpMap.put(ComparatorType.TIMEUUIDTYPE, "TIMEUUID"); 
		
		for (ComparatorType cType : tmpMap.keySet()) {
			
			String value = tmpMap.get(cType);
			
			comparatorToCql3Type.put(cType.getClassName(), value);
			comparatorToCql3Type.put(cType.getTypeName(), value);
			
			cqlToComparatorType.put(value, cType);
		}
	}
	
	public static ComparatorType getComparatorFromCqlType(String cqlTypeString) {
		ComparatorType value = cqlToComparatorType.get(cqlTypeString);
		if (value == null) {
			throw new RuntimeException("Unrecognized cql type: " + cqlTypeString);
		}
		return value;
	}
	
	
	public static String getCqlTypeFromComparator(String comparatorString) {
		String value = comparatorToCql3Type.get(comparatorString);
		if (value == null) {
			throw new RuntimeException("Could not find comparator type string: " + comparatorString);
		}
		return value;
	}
	
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
		List<ColumnDefinition> cluseringKeyList = cfDef.getClusteringKeyColumnDefinitionList();
		
		int componentIndex = 0;
		for (ComponentSerializer<?> component : compositeSerializer.getComponents()) {
			
			Object value = getDynamicColumn(row, component.getSerializer(), cluseringKeyList.get(componentIndex).getName(), cf);
			try {
				component.setFieldValueDirectly(obj, value);
				componentIndex++;
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		return obj;
	}
}
