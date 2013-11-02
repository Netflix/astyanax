package com.netflix.astyanax.cql.util;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;

public class DataTypeMapping {

	public static <T> Object getDynamicColumn(Row row, String columnName, DataType dataType) {
		
		switch(dataType.getName()) {

		case ASCII:
		    return row.getString(columnName);
		case BIGINT:
		    return row.getLong(columnName);
		case BLOB:
		    return row.getBytes(columnName);
		case BOOLEAN:
		    return row.getBool(columnName);
		case COUNTER:
		    return row.getLong(columnName);
		case DECIMAL:
		    return row.getDecimal(columnName);
		case DOUBLE:
		    return row.getDouble(columnName);
		case FLOAT:
		    return row.getFloat(columnName);
		case INET:
		    return row.getInet(columnName);
		case INT:
		    return row.getInt(columnName);
		case TEXT:
		    return row.getString(columnName);
		case TIMESTAMP:
		    return row.getDate(columnName);
		case UUID:
		    return row.getUUID(columnName);
		case VARCHAR:
		    return row.getString(columnName);
		case VARINT:
		    return row.getLong(columnName);
		case TIMEUUID:
		    return row.getUUID(columnName);
		case LIST:
		    throw new UnsupportedOperationException("Collection objects not supported for column: " + columnName);
		case SET:
		    throw new UnsupportedOperationException("Collection objects not supported for column: " + columnName);
		case MAP:
			return row.getMap(columnName, Object.class, Object.class);
		    //throw new UnsupportedOperationException("Collection objects not supported for column: " + columnName);
		case CUSTOM:
		    throw new UnsupportedOperationException("Collection objects not supported for column: " + columnName);
		    
		default:
		    throw new UnsupportedOperationException("Unrecognized object for column: " + columnName);
		}
	}

}
