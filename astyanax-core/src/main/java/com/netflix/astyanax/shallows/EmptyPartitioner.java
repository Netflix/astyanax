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

import java.nio.ByteBuffer;
import java.util.List;

import com.netflix.astyanax.connectionpool.TokenRange;
import com.netflix.astyanax.partitioner.Partitioner;

public class EmptyPartitioner implements Partitioner {

    @Override
    public String getMinToken() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getMaxToken() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getTokenMinusOne(String token) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<TokenRange> splitTokenRange(String first, String last, int count) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<TokenRange> splitTokenRange(int count) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getTokenForKey(ByteBuffer key) {
        // TODO Auto-generated method stub
        return null;
    }

}
