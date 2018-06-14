/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.astyanax.recipes.locks;

import com.netflix.astyanax.model.ByteBufferRange;

/**
 * Strategy used by locking and uniqueness recipes to generate
 * and check lock columns
 * 
 * @author elandau
 *
 * @param <C>
 */
public interface LockColumnStrategy<C> {
    /**
     * Return true if this is a lock column
     * @param c
     * @return
     */
    boolean isLockColumn(C c);
    
    /**
     * Return the ByteBuffer range to use when querying all lock
     * columns in a row
     * @return
     */
    ByteBufferRange getLockColumnRange();
    
    /**
     * Generate a unique lock column
     * @return
     */
    C generateLockColumn();
}
