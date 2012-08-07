package com.netflix.astyanax.impl;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.cassandra.dht.Token;

import com.google.common.base.Function;
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
public class FilteringHostSupplier implements Supplier<Map<Token, List<Host>>> {

    private final Supplier<Map<Token, List<Host>>> sourceSupplier;
    private final Supplier<Map<Token, List<Host>>> filterSupplier;

    public FilteringHostSupplier(Supplier<Map<Token, List<Host>>> sourceSupplier,
            Supplier<Map<Token, List<Host>>> filterSupplier) {
        this.sourceSupplier = sourceSupplier;
        this.filterSupplier = filterSupplier;
    }

    @Override
    public Map<Token, List<Host>> get() {
        Map<Token, List<Host>> filterList = Maps.newHashMap();
        Map<Token, List<Host>> sourceList;
        try {
            filterList = filterSupplier.get();
            sourceList = sourceSupplier.get();
        }
        catch (RuntimeException e) {
            if (filterList != null)
                return filterList;
            throw e;
        }

        final Map<String, Host> lookup = Maps.newHashMap();
        for (Entry<Token, List<Host>> token : filterList.entrySet()) {
            for (Host host : token.getValue()) {
                lookup.put(host.getIpAddress(), host);
                for (String addr : host.getAlternateIpAddresses()) {
                    lookup.put(addr, host);
                }
            }
        }

        Map<Token, List<Host>> response = Maps.newHashMap();
        for (Entry<Token, List<Host>> token : sourceList.entrySet()) {
            response.put(
                    token.getKey(),
                    Lists.newArrayList(Collections2.transform(
                            Collections2.filter(token.getValue(), new Predicate<Host>() {
                                @Override
                                public boolean apply(Host host) {
                                    return lookup.containsKey(host.getIpAddress());
                                }
                            }), new Function<Host, Host>() {
                                @Override
                                public Host apply(Host host) {
                                    return lookup.get(host.getIpAddress());
                                }
                            })));
        }
        return response;
    }

}
