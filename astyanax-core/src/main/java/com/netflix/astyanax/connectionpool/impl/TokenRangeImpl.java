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
package com.netflix.astyanax.connectionpool.impl;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.netflix.astyanax.connectionpool.TokenRange;

/**
 * Impl for {@link TokenRange}
 * 
 * @author elandau
 */
public class TokenRangeImpl implements TokenRange {

    private final String startToken;
    private final String endToken;
    private final List<String> endpoints;

    public TokenRangeImpl(String startToken, String endToken, List<String> endpoints) {
        this.startToken = startToken;
        this.endToken = endToken;
        if (endpoints != null)
            this.endpoints = ImmutableList.copyOf(endpoints);
        else 
            this.endpoints = Lists.newArrayList();
    }

    @Override
    public String getStartToken() {
        return this.startToken;
    }

    @Override
    public String getEndToken() {
        return this.endToken;
    }

    @Override
    public List<String> getEndpoints() {
        return this.endpoints;
    }

    @Override
    public String toString() {
        return "TokenRangeImpl [startToken=" + startToken + ", endToken=" + endToken + ", endpoints=" + endpoints + "]";
    }

}
