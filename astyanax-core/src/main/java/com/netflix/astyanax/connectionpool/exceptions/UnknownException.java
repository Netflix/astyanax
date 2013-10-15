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

public class UnknownException extends OperationException implements IsDeadConnectionException {
    /**
     * 
     */
    private static final long serialVersionUID = -6204702276016512865L;

    public UnknownException(String message) {
        super(message);
    }

    public UnknownException(Throwable t) {
        super(t);
    }

    public UnknownException(String message, Throwable cause) {
        super(message, cause);
    }
}
