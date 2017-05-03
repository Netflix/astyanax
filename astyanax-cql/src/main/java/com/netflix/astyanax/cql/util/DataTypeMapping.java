/*******************************************************************************
 * Copyright 2011 Netflix
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
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
