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
/**
 *  ConsistencyLevel.java
 *  dsmirnov Mar 31, 2011
 */
package com.netflix.astyanax.model;

/**
 * Consistency Level thin abstraction
 * 
 * @author dsmirnov
 * 
 */
public enum ConsistencyLevel {
    CL_ONE, CL_QUORUM, CL_ALL, CL_ANY, CL_EACH_QUORUM, CL_LOCAL_QUORUM, CL_TWO, CL_THREE, CL_LOCAL_ONE;
}
