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
package com.netflix.astyanax.partitioner;

import java.nio.ByteBuffer;
import java.util.List;

import com.netflix.astyanax.connectionpool.TokenRange;

/**
 * Base interface for token partitioning utilities
 * 
 * @author elandau
 *
 */
public interface Partitioner {
    /**
     * @return Return the smallest token in the token space
     */
    String getMinToken();
    
    /**
     * @return Return the largest token in the token space
     */
    String getMaxToken();
    
    /**
     * @return Return the token immediately before this one
     */
    String getTokenMinusOne(String token);
    
    /**
     * Split the token range into N equal size segments and return the start token
     * of each segment
     * 
     * @param first
     * @param last
     * @param count
     */
    List<TokenRange> splitTokenRange(String first, String last, int count);
    
    /**
     * Split the entire token range into 'count' equal size segments
     * @param count
     */
    List<TokenRange> splitTokenRange(int count);
    
    /**
     * Return the token for the specifie key
     * @param key
     */
    String getTokenForKey(ByteBuffer key);
    
}
