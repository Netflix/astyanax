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
package com.netflix.astyanax.test;

import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.netflix.astyanax.connectionpool.TokenRange;
import com.netflix.astyanax.connectionpool.impl.EndpointDetailsImpl;

public class TestTokenRange implements TokenRange {
    private String start;
    private String end;
    private List<String> endpoints;

    public TestTokenRange(String start, String end, List<String> endpoints) {
        this.start = start;
        this.end = end;
        this.endpoints = endpoints;
    }

    @Override
    public String getStartToken() {
        return start;
    }

    @Override
    public String getEndToken() {
        return end;
    }

    @Override
    public List<String> getEndpoints() {
        return endpoints;
    }

    @Override
    public List<EndpointDetails> getEndpointDetails() {
        return Lists.transform(endpoints, new Function<String, EndpointDetails>() {
            @Override
            public EndpointDetails apply(String host) {
                return new EndpointDetailsImpl(host, "dataCenter1", "rack1");
            }
        });
    }
}
