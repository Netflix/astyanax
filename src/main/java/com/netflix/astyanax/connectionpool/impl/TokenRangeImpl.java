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
import com.netflix.astyanax.connectionpool.Endpoint;
import com.netflix.astyanax.connectionpool.TokenRange;

public class TokenRangeImpl implements TokenRange {

    private final String startToken;
    private final String endToken;
    private final List<Endpoint> endpoints;

    public TokenRangeImpl(String startToken, String endToken, List<Endpoint> endpoints) {
        this.startToken = startToken;
        this.endToken = endToken;
        this.endpoints = ImmutableList.copyOf(endpoints);
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
    public List<Endpoint> getEndpoints() {
        return this.endpoints;
    }

    @Override
    public String toString() {
        return "TokenRangeImpl [startToken=" + startToken + ", endToken=" + endToken + ", endpoints=" + endpoints + "]";
    }

}
