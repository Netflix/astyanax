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

public class ThrottledException extends ConnectionException implements IsRetryableException {
    private static final long serialVersionUID = 1257641642867458438L;

    public ThrottledException(String message) {
        super(message);
    }

    public ThrottledException(Throwable t) {
        super(t);
    }

    public ThrottledException(String message, Throwable cause) {
        super(message, cause);
    }
}
