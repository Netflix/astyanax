package com.netflix.astyanax.impl;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.astyanax.connectionpool.Host;

/**
 * Node discovery supplier that only return suppliers that come from both sources
 * @author elandau
 *
 */
public class FilteringHostSupplier implements Supplier<Map<BigInteger, List<Host>>> {

	private final Supplier<Map<BigInteger, List<Host>>> sourceSupplier;
	private final Supplier<Map<BigInteger, List<Host>>> filterSupplier;
	
	public FilteringHostSupplier(Supplier<Map<BigInteger, List<Host>>> sourceSupplier, Supplier<Map<BigInteger, List<Host>>> filterSupplier) {
		this.sourceSupplier = sourceSupplier;
		this.filterSupplier = filterSupplier;
	}
	
	@Override
	public Map<BigInteger, List<Host>> get() {
		Map<BigInteger, List<Host>> filterList = Maps.newHashMap();
		Map<BigInteger, List<Host>> sourceList;
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
		for (Entry<BigInteger, List<Host>> token : filterList.entrySet()) {
			for (Host host : token.getValue()) {
				lookup.put(host.getIpAddress(), host);
				for (String addr : host.getAlternateIpAddresses()) {
					lookup.put(addr, host);
				}
			}
		}
		
		Map<BigInteger, List<Host>> response = Maps.newHashMap();
		for (Entry<BigInteger, List<Host>> token : sourceList.entrySet()) {
			response.put(token.getKey(), Lists.newArrayList(Collections2.transform(
				Collections2.filter(token.getValue(), 
						new Predicate<Host>() {
							@Override
							public boolean apply(Host host) {
								return lookup.containsKey(host.getIpAddress());
							}
		        		}),
				new Function<Host, Host>() {
					@Override
					public Host apply(Host host) {
						return lookup.get(host.getIpAddress());
					}
				}
			)));
		}
		return response;
	}

}
