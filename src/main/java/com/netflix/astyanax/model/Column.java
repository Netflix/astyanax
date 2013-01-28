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
package com.netflix.astyanax.model;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

import com.netflix.astyanax.Serializer;

/**
 * Common interface for extracting column values after a query.
 * 
 * @author elandau
 * 
 *         TODO Add getIntValue, getDoubleValue, ...
 * 
 * @param <C>
 *            Column name type
 */
public interface Column<C> {

    /**
     * Column or super column name
     * 
     * @return
     */
    C getName();

    /**
     * Return the raw byet buffer for the column name
     * 
     * @return
     */
    ByteBuffer getRawName();

    /**
     * Returns the column timestamp. Not to be confused with column values that
     * happen to be time values.
     * 
     * @return
     */
    long getTimestamp();

    /**
     * Return the value
     * 
     * @param <V>
     *            value type
     * @return
     * @throws NetflixCassandraException
     */
    <V> V getValue(Serializer<V> valSer);

    /**
     * Return value as a string
     * 
     * @return
     */
    String getStringValue();
    
    /**
     * Return a string that was stored as a compressed blob
     * @return
     */
    String getCompressedStringValue();
    
    /**
     * Return value as an integer
     * 
     * @return
     */
    byte getByteValue();
    
    /**
     * Return value as an integer
     * 
     * @return
     */
    short getShortValue();

    /**
     * Return value as an integer
     * 
     * @return
     */
    int getIntegerValue();
    
    /**
     * Return value as a float
     * 
     * @return
     */
    float getFloatValue();

    /**
     * Return value as a double
     * 
     * @return
     */
    double getDoubleValue();

    /**
     * Return value as a long. Use this to get the value of a counter column
     * 
     * @return
     */
    long getLongValue();

    /**
     * Get the raw byte[] value
     * 
     * @return
     */
    byte[] getByteArrayValue();

    /**
     * Get value as a boolean
     * 
     * @return
     */
    boolean getBooleanValue();

    /**
     * Get the raw ByteBuffer value
     * 
     * @return
     */
    ByteBuffer getByteBufferValue();

    /**
     * Get the value as a date object
     * 
     * @return
     */
    Date getDateValue();

    /**
     * Get the value as a UUID
     * 
     * @return
     */
    UUID getUUIDValue();

    /**
     * Get columns in the case of a super column. Will throw an exception if
     * this is a regular column Valid only if isCompositeColumn returns true
     * 
     * @param <C2>
     *            Type of column names for sub columns
     * @deprecated Super columns should be replaced with composite columns
     * @return
     */
    @Deprecated
    <C2> ColumnList<C2> getSubColumns(Serializer<C2> ser);

    /**
     * Returns true if the column contains a list of child columns, otherwise
     * the column contains a value.
     * 
     * @deprecated Super columns should be replaced with composite columns
     * @return
     */
    @Deprecated
    boolean isParentColumn();

    /**
     * Get the TTL for this column.  
     * @return TTL in seconds or 0 if no ttl was set
     */
    int getTtl();
 
    /**
     * Determine whether the column has a value. 
     * 
     * @return  True if column has a value or false if value is null or an empty byte array.
     */
    boolean hasValue();
}
