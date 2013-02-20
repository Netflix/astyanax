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
package com.netflix.astyanax.shallows;

import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.OperationFilterFactory;

public class EmptyOperationFilterFactory implements OperationFilterFactory {
    private final static OperationFilterFactory instance = new EmptyOperationFilterFactory();
    
    public static OperationFilterFactory getInstance() {
        return instance;
    }
            
    @Override
    public <R, CL> Operation<R, CL> attachFilter(Operation<R, CL> operation) {
        return operation;
    }
}
