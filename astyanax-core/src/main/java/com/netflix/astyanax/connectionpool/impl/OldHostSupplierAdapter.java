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
package com.netflix.astyanax.connectionpool.impl;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.astyanax.connectionpool.Host;

public class OldHostSupplierAdapter implements Supplier<List<Host>> {
    private final Supplier<Map<BigInteger, List<Host>>> source;
    
    public OldHostSupplierAdapter(Supplier<Map<BigInteger, List<Host>>> source) {
        this.source = source;
    }
    
    @Override
    public List<Host> get() {
        Map<BigInteger, List<Host>> tokenHostMap = source.get();
        Set<Host> hosts = Sets.newHashSet();
        for (Entry<BigInteger, List<Host>> entry : tokenHostMap.entrySet()) { 
            for (Host host : entry.getValue()) {
                if (!hosts.contains(host)) {
                    hosts.add(host);
                }
                
                String token = entry.getKey().toString();
                host.getTokenRanges().add(new TokenRangeImpl(token, token, null));
            }
        }
        return Lists.newArrayList(hosts);
    }

}
