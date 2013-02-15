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
