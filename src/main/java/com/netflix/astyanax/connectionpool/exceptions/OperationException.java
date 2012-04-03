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
package com.netflix.astyanax.connectionpool.exceptions;

/**
 * Application exception for an operation executed within the context of the
 * connection pool. An application exception varies from other
 * ConnectionException in that it will immediately roll up to the client and
 * cannot fail over. Examples of application exceptions are invalid request
 * formats.
 * 
 * @author elandau
 * 
 */
public class OperationException extends ConnectionException {
    /**
     * 
     */
    private static final long serialVersionUID = -369210846483113052L;

    public OperationException(String message) {
        super(message);
    }

    public OperationException(Throwable t) {
        super(t);
    }

    public OperationException(String message, Throwable cause) {
        super(message, cause);
    }

}
