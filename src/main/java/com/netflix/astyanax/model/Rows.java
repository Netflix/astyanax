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

import java.util.Collection;

/**
 * Interface to a collection or Rows with key type K and column type C. The rows
 * can be either super or standard, but not both.
 * 
 * @author elandau
 * 
 * @param <K>
 * @param <C>
 */
public interface Rows<K, C> extends Iterable<Row<K, C>> {
    /**
     * Return all row keys in the set
     * @return
     */
    Collection<K> getKeys();
    
    /**
     * Return the row for a specific key. Will return an exception if the result
     * set is a list and not a lookup.
     * 
     * @param key
     * @return
     */
    Row<K, C> getRow(K key);

    /**
     * Return a row by it's index in the response.
     * 
     * @param i
     */
    Row<K, C> getRowByIndex(int i);

    /**
     * Get the number of rows in the list
     * 
     * @return integer representing the number of rows in the list
     */
    int size();

    /**
     * Determine if the row list has data
     * 
     * @return
     */
    boolean isEmpty();
}
