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
package com.netflix.astyanax.ddl;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;

/**
 * Interface to get/set a single column definition. The column definition is
 * only valid within the context of a ColumnFamilyDefinition
 * 
 * @author elandau
 * 
 */
public interface ColumnDefinition {
    /**
     * Sets the column string name
     * 
     * @param name
     * @return
     */
    ColumnDefinition setName(String name);

    /**
     * Sets the column byte array name
     * 
     * @param name
     * @return
     */
    ColumnDefinition setName(byte[] name);

    /**
     * Sets the column byte buffer name
     * 
     * @param name
     * @return
     */
    ColumnDefinition setName(ByteBuffer name);

    /**
     * Sets the validation class for the column values. See ComparatorType for
     * possible values. Setting the validation class here makes it possible to
     * have different values types per column within the same column family.
     * 
     * @param value
     * @return
     */
    ColumnDefinition setValidationClass(String value);

    /**
     * Sets an index on this column.
     * 
     * @param name
     *            Name of index
     * @param type
     *            "KEYS"
     * @return
     */
    ColumnDefinition setIndex(String name, String type);

    /**
     * Sets a keys index on this column
     * 
     * @param name
     * @return
     */
    ColumnDefinition setKeysIndex(String name);

    /**
     * Get the column name
     * 
     * @return
     */
    String getName();

    /**
     * Get the raw column name. In most cases the column name is a string but
     * the actual column name is stored as a byte array
     * 
     * @return
     */
    ByteBuffer getRawName();

    /**
     * Return the value validation type. See ComparatorType for possible values.
     * 
     * @return
     */
    String getValidationClass();

    /**
     * Return the index name
     * 
     * @return
     */
    String getIndexName();

    /**
     * Return the index type. At the time of this writing only KEYS index is
     * supported
     * 
     * @return
     */
    String getIndexType();

    /**
     * Returns true if there is an index on this column
     * 
     * @return
     */
    boolean hasIndex();

    /**
     * Get a map of all options associated with this column
     * @return
     */
    Map<String, String> getOptions();

    /**
     * Get an option
     * @param name          Option name (TODO: Document these)
     *                      'class_name' - com.datastax.bdp.cassandra.index.solr.SolrSecondaryIndex
     * @param defaultValue  Default value to return if option not found
     * @return 
     */
    String getOption(String name, String defaultValue);
    
    /**
     * Set all extra options for this column.  Will override any previous values.
     * @param index_options
     * @return
     */
    ColumnDefinition setOptions(Map<String, String> index_options);

    /**
     * Set an option
     * @param name
     * @param value
     * @return Previous value or null if not previously set
     */
    String setOption(String name, String value);

    /**
     * Return list of valid field names
     * @return
     */
    Collection<String> getFieldNames();
    
    /**
     * Get metadata for all fields
     * @return
     */
    Collection<FieldMetadata> getFieldsMetadata();

    /**
     * Return a field value by name
     * @param name
     * @return
     */
    Object getFieldValue(String name);

    /**
     * Set a field value by name
     * 
     * @param name
     * @param value
     * @return
     */
    ColumnDefinition setFieldValue(String name, Object value);
    
    ColumnDefinition setFields(Map<String, Object> fields);
}
