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
import java.util.Collection;
import java.util.Date;
import java.util.UUID;

import com.netflix.astyanax.Serializer;

/**
 * Interface to a list of columns.
 * 
 * @author elandau
 * 
 * @param <C>
 *            Data type for column names
 */
public interface ColumnList<C> extends Iterable<Column<C>> {
    /**
     * Return the column names
     */
    Collection<C> getColumnNames();
    
    /**
     * Queries column by name
     * 
     * @param columnName
     * @return an instance of a column or null if not found
     * @throws Exception
     */
    Column<C> getColumnByName(C columnName);

    /**
     * Return value as a string
     * 
     * @return
     */
    String getStringValue(C columnName, String defaultValue);

    /**
     * Get a string that was stored as a compressed blob
     * @param columnName
     * @param defaultValue
     * @return
     */
    String getCompressedStringValue(C columnName, String defaultValue);
    
    /**
     * Return value as an integer
     * 
     * @return
     */
    Integer getIntegerValue(C columnName, Integer defaultValue);

    /**
     * Return value as a double
     * 
     * @return
     */
    Double getDoubleValue(C columnName, Double defaultValue);

    /**
     * Return value as a long. Use this to get the value of a counter column
     * 
     * @return
     */
    Long getLongValue(C columnName, Long defaultValue);

    /**
     * Get the raw byte[] value
     * 
     * @return
     */
    byte[] getByteArrayValue(C columnName, byte[] defaultValue);

    /**
     * Get value as a boolean
     * 
     * @return
     */
    Boolean getBooleanValue(C columnName, Boolean defaultValue);

    /**
     * Get the raw ByteBuffer value
     * 
     * @return
     */
    ByteBuffer getByteBufferValue(C columnName, ByteBuffer defaultValue);

    /**
     * Get a value with optional default using a specified serializer
     * @param <T>
     * @param columnName
     * @param serializer
     * @param defaultValue
     * @return
     */
    <T> T getValue(C columnName, Serializer<T> serializer, T defaultValue);
    
    /**
     * Get the value as a date object
     * 
     * @return
     */
    Date getDateValue(C columnName, Date defaultValue);

    /**
     * Get the value as a UUID
     * 
     * @return
     */
    UUID getUUIDValue(C columnName, UUID defaultValue);

    /**
     * Queries column by index
     * 
     * @param idx
     * @return
     * @throws NetflixCassandraException
     */
    Column<C> getColumnByIndex(int idx);

    /**
     * Return the super column with the specified name
     * 
     * @param <C2>
     * @param columnName
     * @param colSer
     * @return
     * @throws NetflixCassandraException
     * @deprecated Super columns should be replaced with composite columns
     */
    <C2> Column<C2> getSuperColumn(C columnName, Serializer<C2> colSer);

    /**
     * Get super column by index
     * 
     * @param idx
     * @return
     * @throws NetflixCassandraException
     * @deprecated Super columns should be replaced with composite columns
     */
    <C2> Column<C2> getSuperColumn(int idx, Serializer<C2> colSer);

    /**
     * Indicates if the list of columns is empty
     * 
     * @return
     */
    boolean isEmpty();

    /**
     * returns the number of columns in the row
     * 
     * @return
     */
    int size();

    /**
     * Returns true if the columns are super columns with subcolumns. If true
     * then use getSuperColumn to call children. Otherwise call getColumnByIndex
     * and getColumnByName to get the standard columns in the list.
     * 
     * @return
     * @deprecated Super columns should be replaced with composite columns
     */
    boolean isSuperColumn();
}
