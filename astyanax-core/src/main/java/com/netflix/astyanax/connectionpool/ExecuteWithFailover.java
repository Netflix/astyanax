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

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.AbstractHostPartitionConnectionPool;

/**
 * Interface that encapsulates functionality to execute an {@link Operation} with a failover strategy as well. 
 * 
 * This class is used when attempting to find a healthy {@link Connection} from a set of {@link HostConnectionPool}(s) for a set of {@link Host}(s)
 * This can be used by an extending class of the {@link AbstractHostPartitionConnectionPool} which reliably tries to execute an {@link Operation} 
 * within the {@link AbstractHostPartitionConnectionPool#executeWithFailover(Operation, com.netflix.astyanax.retry.RetryPolicy)} method for the 
 * given host partitions.
 * 
 * @see {@link AbstractHostPartitionConnectionPool} for references to this class. 
 * 
 * @author elandau
 *
 * @param <CL>
 * @param <R>
 */
public interface ExecuteWithFailover<CL, R> {
    OperationResult<R> tryOperation(Operation<CL, R> operation) throws ConnectionException;
}
