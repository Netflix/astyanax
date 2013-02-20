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
package com.netflix.astyanax.recipes.locks;

public class BusyLockException extends Exception {
    private static final long serialVersionUID = -6818914810045830278L;

    public BusyLockException(Exception e) {
        super(e);
    }

    public BusyLockException(String message, Exception e) {
        super(message, e);
    }

    public BusyLockException(String message) {
        super(message);
    }
}
