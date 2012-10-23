package com.netflix.astyanax.impl;

import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

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

    public FilteringHostSupplier(Supplier<List<Host>> sourceSupplier,
            Supplier<List<Host>> filterSupplier) {
        this.sourceSupplier = sourceSupplier;
        this.filterSupplier = filterSupplier;
    }

    @Override
    public List<Host> get() {
        // Get a list of hosts from two sources and use the first to filter
        // out hosts from the second
        List<Host> filterList = Lists.newArrayList();
        List<Host> sourceList;
        try {
            filterList = filterSupplier.get();
            sourceList = sourceSupplier.get();
        }
        catch (RuntimeException e) {
            if (filterList != null)
                return filterList;
            throw e;
        }

        // Generate a lookup of all alternate IP addresses for the hosts in the
        // filter list
        final Map<String, Host> lookup = Maps.newHashMap();
        for (Host host : filterList) {
            lookup.put(host.getIpAddress(), host);
            for (String addr : host.getAlternateIpAddresses()) {
                lookup.put(addr, host);
            }
        }

        return Lists.newArrayList(Collections2.filter(sourceList, new Predicate<Host>() {
            @Override
            public boolean apply(@Nullable Host host) {
                return lookup.containsKey(host.getIpAddress());
            }
        }));
    }

}
