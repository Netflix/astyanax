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
package com.netflix.astyanax;

/**
 * Representation for a user/password used to log into a keyspace.
 * 
 * @author elandau
 * 
 */
public interface AuthenticationCredentials {
    /**
     * @return The username
     */
    String getUsername();

    /**
     * @return The password
     */
    String getPassword();

    /**
     * @return Array of all custom attribute names
     */
    String[] getAttributeNames();

    /**
     * Retrieve a single attribute by name
     * 
     * @param name  Attribute name
     */
    Object getAttribute(String name);
}
