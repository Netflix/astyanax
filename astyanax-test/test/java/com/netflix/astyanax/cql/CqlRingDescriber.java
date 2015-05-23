package com.netflix.astyanax.cql;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.netflix.astyanax.connectionpool.TokenRange;
import com.netflix.astyanax.connectionpool.impl.TokenRangeImpl;
import com.netflix.astyanax.partitioner.Murmur3Partitioner;

/**
 * Helper class that parses the ring information from the system and peers table. 
 * Note that it maintains a cached reference and allows the user to either reuse the cache or refresh the cahe.
 *  
 * @author poberai
 */
public class CqlRingDescriber {

	private final AtomicReference<List<TokenRange>>  cachedReference = new AtomicReference<List<TokenRange>>(null);
	
	private static CqlRingDescriber Instance = new CqlRingDescriber();
	
	private CqlRingDescriber() {
	}
	
	public static CqlRingDescriber getInstance() {
		return Instance; 
	}
	
	public List<TokenRange> getTokenRanges(Session session, boolean cached) {
		
		if (cached && cachedReference.get() != null) {
			return cachedReference.get();
		}
		
		// else get the actual token range list and then cache it
		List<TokenRange> ranges = getTokenRanges(session, null, null);
		cachedReference.set(ranges);
		return ranges;
	}
	
	public List<TokenRange> getTokenRanges(Session session, String dc, String rack) {
		
		List<HostInfo> hosts = new ArrayList<HostInfo>();

		Statement query = QueryBuilder.select().all().from("system", "local"); 
		ResultSet resultSet = session.execute(query);
		hosts.add(new HostInfo(resultSet.one(), resultSet));
		
		query = QueryBuilder.select("peer", "data_center", "host_id", "rack", "tokens").from("system", "peers"); 
		resultSet = session.execute(query);
		for (Row row : resultSet.all()) {
			hosts.add(new HostInfo(row, null));
		}
		
		Collections.sort(hosts);

		List<TokenRange> ranges = new ArrayList<TokenRange>();

		for (int index = 0; index<hosts.size(); index++) {

			HostInfo thisNode = hosts.get(index);

			List<String> endpoints = new ArrayList<String>();

			if (matchNode(dc, rack, thisNode)) {
				endpoints.add(thisNode.endpoint); // the primary range owner
			}

			// secondary node
			int nextIndex = getNextIndex(index, hosts.size());
			if (nextIndex != index) {

				HostInfo nextNode = hosts.get(nextIndex);
				if (matchNode(dc, rack, nextNode)) {
					endpoints.add(nextNode.endpoint);
				}

				// tertiary node
				nextIndex = getNextIndex(nextIndex, hosts.size());
				nextNode = hosts.get(nextIndex);
				if (matchNode(dc, rack, nextNode)) {
					endpoints.add(nextNode.endpoint);
				}
			}
			int prevIndex = getPrevIndex(index, hosts.size());
			String startToken = hosts.get(prevIndex).token.toString();
			String endToken = thisNode.token.toString();

			if (startToken.equals(endToken)) {
				// TOKENS are the same. This happens during testing. 
				startToken = Murmur3Partitioner.get().getMinToken();
				endToken = Murmur3Partitioner.get().getMinToken();
				
			}
			ranges.add(new TokenRangeImpl(startToken, endToken, endpoints));
		}

		return ranges;
	}
	
	private boolean matchNode(String dc, String rack, HostInfo host) {
		
		if (dc == null && rack == null) {
			return true; // node matches since there is no filter
		}
		
		if (dc != null && !dc.equals(host.datacenter)) {
			return false; // wrong dc 
		}
		
		if (rack != null && !rack.equals(host.rack)) {
			return false; // wrong rack
		}
		
		return true; // match!
	}
	
	private int getNextIndex(int i, int n) {
		
		int next = ++i;
		if (i >= n) {
			return 0; 
		} else {
			return next;
		}
	}
	
	private int getPrevIndex(int i, int n) {
		
		int prev = --i;
		if (i < 0) {
			return n-1; 
		} else {
			return prev;
		}
	}

	private class HostInfo implements Comparable<HostInfo> {
		
		private final BigInteger token;
		private final String endpoint; 
		private final UUID hostId; 
		private final String datacenter;
		private final String rack; 
		
		private HostInfo(Row row, ResultSet rs) {
			
			if (row == null) {
				throw new RuntimeException("RS is empty for system.local query");
			}
		
			Set<String> tokens = row.getSet("tokens", String.class);
			
			String theToken = tokens.iterator().next();
			token = new BigInteger(theToken);
			
			hostId = row.getUUID("host_id");
			datacenter = row.getString("data_center");
			rack = row.getString("rack");

			if (rs != null) {
				endpoint = rs.getExecutionInfo().getQueriedHost().getAddress().getHostAddress();
			} else {
				endpoint = row.getInet("peer").getHostAddress();
			}
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((token == null) ? 0 : token.hashCode());
			result = prime * result + ((endpoint == null) ? 0 : endpoint.hashCode());
			result = prime * result + ((hostId == null) ? 0 : hostId.hashCode());
			result = prime * result + ((datacenter == null) ? 0 : datacenter.hashCode());
			result = prime * result + ((rack == null) ? 0 : rack.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			
			if (this == obj) return true;
			if (obj == null) return false;
			if (getClass() != obj.getClass()) return false;
			
			HostInfo other = (HostInfo) obj;
			boolean equal = true; 
			
			equal &= (token != null) ? token.equals(other.token) : (other.token == null);
			equal &= (endpoint != null) ? endpoint.equals(other.endpoint) : (other.endpoint == null);
			equal &= (hostId != null) ? hostId.equals(other.hostId) : (other.hostId == null);
			equal &= (datacenter != null) ? datacenter.equals(other.datacenter) : (other.datacenter == null);
			equal &= (rack != null) ? rack.equals(other.rack) : (other.rack == null);
			
			return equal;
		}

		@Override
		public String toString() {
			return "HostInfo [token=" + token + ", endpoint=" + endpoint
					+ ", hostId=" + hostId.toString() + ", datacenter=" + datacenter
					+ ", rack=" + rack + "]";
		}

		@Override
		public int compareTo(HostInfo o) {
			return this.token.compareTo(o.token);
		}
	}
}
