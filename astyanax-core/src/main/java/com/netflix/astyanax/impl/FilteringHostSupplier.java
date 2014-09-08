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
package com.netflix.astyanax.impl;

import java.util.List;
import java.util.Map;

import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.astyanax.connectionpool.Host;

/**
 * Node discovery supplier that only return suppliers that come from both
 * sources
 * 
 * @author elandau
 * 
 */
public class FilteringHostSupplier implements Supplier<List<Host>> {

    private final Supplier<List<Host>> sourceSupplier;
    private final Supplier<List<Host>> filterSupplier;

    public FilteringHostSupplier(
            Supplier<List<Host>> filterSupplier,    // This is a ring describe
            Supplier<List<Host>> sourceSupplier) {
        this.sourceSupplier = sourceSupplier;
        this.filterSupplier = filterSupplier;
    }

    @Override
    public List<Host> get() {
        // Get a list of hosts from two sources and use the first to filter
        // out hosts from the second
        List<Host> filterList = Lists.newArrayList();
        List<Host> sourceList = Lists.newArrayList();
        try {
            sourceList = sourceSupplier.get();
            filterList = filterSupplier.get();
        }
        catch (RuntimeException e) {
            if (sourceList != null)
                return sourceList;
            throw e;
        }
        
        if (filterList.isEmpty())
            return sourceList;
        
        // Generate a lookup of all alternate IP addresses for the hosts in the
        // filter list
        final Map<String, Host> lookup = Maps.newHashMap();
        for (Host host : filterList) {
            lookup.put(host.getIpAddress(), host);
            for (String addr : host.getAlternateIpAddresses()) {
                lookup.put(addr, host);
            }
        }

        List<Host> result = Lists.newArrayList(Collections2.filter(sourceList, new Predicate<Host>() {
            @Override
            public boolean apply(Host host) {
                Host foundHost = lookup.get(host.getHostName());
                if (foundHost == null) {
                    for (String addr : host.getAlternateIpAddresses()) {
                        foundHost = lookup.get(addr);
                        if (foundHost != null) {
                            break;
                        }
                    }
                }
                
                if (foundHost != null) {
                    host.setTokenRanges(foundHost.getTokenRanges());
                    return true;
                }
                
                return false;
            }
        }));
        
        if (result.isEmpty()) 
            return sourceList;
        return result;
    }

}
