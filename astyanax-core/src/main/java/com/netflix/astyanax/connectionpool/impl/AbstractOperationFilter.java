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
package com.netflix.astyanax.connectionpool.impl;

import java.nio.ByteBuffer;

import com.netflix.astyanax.connectionpool.ConnectionContext;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

/**
 * 
 * Class that wraps an {@link Operation} to provide extra functionality. It can be used by extending class to wrap operation executions
 * and then decorate the execute functionality with their own logic 
 * 
 * @author elandau
 *
 * @param <CL>
 * @param <R>
 */
public class AbstractOperationFilter<CL, R> implements Operation<CL, R>{

    private Operation<CL, R> next;
    
    public AbstractOperationFilter(Operation<CL, R> next) {
        this.next = next;
    }
    
    @Override
    public R execute(CL client, ConnectionContext state) throws ConnectionException {
        return next.execute(client, state);
    }
    
    @Override
    public ByteBuffer getRowKey() {
        return next.getRowKey();
    }

    @Override
    public String getKeyspace() {
        return next.getKeyspace();
    }

    @Override
    public Host getPinnedHost() {
        return next.getPinnedHost();
    }

}
