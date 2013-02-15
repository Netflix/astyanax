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
package com.netflix.astyanax.query;

import com.netflix.astyanax.Execution;
import com.netflix.astyanax.model.CqlResult;

/**
 * Interface for executing a CQL query.
 * 
 * @author elandau
 * 
 * @param <K>
 * @param <C>
 */
public interface CqlQuery<K, C> extends Execution<CqlResult<K, C>> {
    /**
     * Turns on compression for the response
     * 
     * @return
     */
    CqlQuery<K, C> useCompression();
    
    /**
     * Prepares the provided CQL statement.  The statement is not executed 
     * here.  Call, withPreparedStatement to execute the prepared statement
     * with variables.
     * @return
     */
    PreparedCqlQuery<K,C> asPreparedStatement();
}
