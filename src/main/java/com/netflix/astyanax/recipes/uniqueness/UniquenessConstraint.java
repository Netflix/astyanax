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
package com.netflix.astyanax.recipes.uniqueness;

import com.google.common.base.Function;
import com.netflix.astyanax.MutationBatch;

public interface UniquenessConstraint {
    /**
     * Acquire the row(s) for uniqueness. Call release() when the uniqueness on
     * the row(s) is no longer needed, such as when deleting the rows.
     * 
     * @throws NotUniqueException
     * @throws Exception
     */
    void acquire() throws NotUniqueException, Exception;

    /**
     * Release the uniqueness lock for this row.  Only call this when you no longer
     * need the uniqueness lock
     * 
     * @throws Exception
     */
    void release() throws Exception;

    /**
     * Acquire the uniqueness constraint and apply the final mutation if the 
     * row is found to be unique
     * @param mutation
     * @throws NotUniqueException
     * @throws Exception
     * 
     * @deprecated This method doesn't actually work because the MutationBatch timestamp being behind
     */
    @Deprecated
    void acquireAndMutate(MutationBatch mutation) throws NotUniqueException, Exception;

    /**
     * Acquire the uniqueness constraint and call the mutate callback to fill a mutation.
     * 
     * @param callback
     * @throws NotUniqueException
     * @throws Exception
     */
    void acquireAndApplyMutation(Function<MutationBatch, Boolean> callback) throws NotUniqueException, Exception;
}
