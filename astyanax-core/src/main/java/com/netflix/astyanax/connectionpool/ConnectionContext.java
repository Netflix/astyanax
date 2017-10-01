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
package com.netflix.astyanax.connectionpool;

/**
 * Context specific to a connection.  This interface makes it possible to store
 * connection specific state such as prepared CQL statement ids.
 * @author elandau
 *
 */
public interface ConnectionContext {
    /**
     * Set metadata identified by 'key'
     * @param key
     * @param obj
     */
    public void    setMetadata(String key, Object obj);
    
    /**
     * @return Get metadata stored by calling setMetadata
     * @param key
     */
    public Object  getMetadata(String key);
    
    /**
     * @return Return true if the metadata with the specified key exists.
     * @param key
     */
    public boolean hasMetadata(String key);
}
