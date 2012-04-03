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

public enum NodeDiscoveryType {
    /**
     * Discover nodes exclusively from doing a ring describe
     */
    RING_DESCRIBE,

    /**
     * Discover nodes exclusively from an external node discovery service
     */
    DISCOVERY_SERVICE,

    /**
     * Intersect ring describe and nodes from an external service. This solve
     * the multi-region ring describe problem where ring describe returns nodes
     * from other regions.
     */
    TOKEN_AWARE,

    /**
     * Use only nodes in the list of seeds
     */
    NONE
}
