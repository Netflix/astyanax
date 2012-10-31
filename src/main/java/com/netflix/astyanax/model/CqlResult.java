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

/**
 * Interface for a CQL query result. The result can either be a set of rows or a
 * count/number.
 * 
 * @author elandau
 * 
 * @param <K>
 * @param <C>
 */
public interface CqlResult<K, C> {
    Rows<K, C> getRows();

    int getNumber();

    boolean hasRows();

    boolean hasNumber();
}
