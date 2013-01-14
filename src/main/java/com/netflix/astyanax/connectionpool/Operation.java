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

import java.nio.ByteBuffer;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

/**
 * Callback interface to perform an operation on a client associated with a
 * connection pool's connection resource
 * 
 * @author elandau
 * 
 * @param <C>
 * @param <R>
 */
public interface Operation<CL, R> {
    /**
     * Execute the operation on the client object and return the results.
     * 
     * @param client - The client object
     * @param state  - State and metadata specific to the connection
     * @return
     * @throws ConnectionException
     */
    R execute(CL client, ConnectionContext state) throws ConnectionException;

    /**
     * Return the unique key on which the operation is performed or null if the
     * operation is performed on multiple keys.
     * 
     * @return
     */
    ByteBuffer getRowKey();
    
    /**
     * Return keyspace for this operation. Return null if using the current
     * keyspace, or a keyspace is not needed for the operation.
     * 
     * @return
     */
    String getKeyspace();

    /**
     * Return the host to run on or null to select a host using the load
     * blancer. Failover is disabled for this scenario.
     * 
     * @param host
     * @return
     */
    Host getPinnedHost();
}
