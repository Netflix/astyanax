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

public class NotUniqueException extends Exception {
    /**
     * 
     */
    private static final long serialVersionUID = -3735805268823536495L;

    public NotUniqueException(Exception e) {
        super(e);
    }

    public NotUniqueException(String message, Exception e) {
        super(message, e);
    }

    public NotUniqueException(String message) {
        super(message);
    }
}
