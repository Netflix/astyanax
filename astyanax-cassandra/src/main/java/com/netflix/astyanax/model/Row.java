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

/**
 * Instance of a row with key type K and column name type C. Child columns can
 * be either standard columns or super columns
 * 
 * @author elandau
 * 
 * @param <K>
 * @param <C>
 */
public interface Row<K, C> {
    /**
     * Return the key value
     * 
     * @return
     */
    K getKey();

    /**
     * Return the raw byte buffer for this key
     * 
     * @return
     */
    ByteBuffer getRawKey();

    /**
     * Child columns of the row. Note that if a ColumnPath was provided to a
     * query these will be the columns at the column path location and not the
     * columns at the root of the row.
     * 
     * @return
     */
    ColumnList<C> getColumns();
}
