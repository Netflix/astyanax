package com.netflix.astyanax.impl;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.*;
import com.netflix.astyanax.connectionpool.Host;

import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Node discovery supplier that filters out nodes that are not in the same data center as the initial/seed hosts.
 *
 * @author Daniel Norberg (daniel.norberg@gmail.com)
 *
 */
public class DatacenterFilteringHostSupplier implements Supplier<Map<BigInteger, List<Host>>> {
    private final Collection<Host> initialHosts;
    private final Supplier<Map<BigInteger, List<Host>>> sourceSupplier;

    public DatacenterFilteringHostSupplier(Collection<Host> initialHosts,
                                           Supplier<Map<BigInteger, List<Host>>> sourceSupplier) {
        this.initialHosts = initialHosts;
        this.sourceSupplier = sourceSupplier;
    }

    @Override
    public Map<BigInteger, List<Host>> get() {
        Map<BigInteger, List<Host>> sourceList = sourceSupplier.get();

        Set<String> initialHostAddresses = Sets.newHashSet(Collections2.transform(initialHosts, new Function<Host, String>() {
            @Override
            public String apply(Host host) {
                return host.getIpAddress();
            }
        }));

        // Adopt the datacenter designation of the first initial host in the ring
        final String datacenter = resolveDatacenter(sourceList, initialHostAddresses);

        return Maps.transformEntries(sourceList, new Maps.EntryTransformer<BigInteger, List<Host>, List<Host>>() {
            @Override
            public List<Host> transformEntry(@Nullable BigInteger bigInteger, @Nullable List<Host> hosts) {
                return ImmutableList.copyOf(Iterables.filter(hosts,  new Predicate<Host>() {
                    @Override
                    public boolean apply(Host host) {
                        String hostDatacenter = host.getDatacenter();
                        return datacenter == null || (hostDatacenter != null && hostDatacenter.equals(datacenter));
                    }
                }));
            }
        });
    }

    private String resolveDatacenter(Map<BigInteger, List<Host>> sourceList, Set<String> initialHostAddresses) {
        for (List<Host> hosts : sourceList.values())
            for (Host host : hosts)
                if (initialHostAddresses.contains(host.getIpAddress()))
                    return host.getDatacenter();
        return null;
    }
}
