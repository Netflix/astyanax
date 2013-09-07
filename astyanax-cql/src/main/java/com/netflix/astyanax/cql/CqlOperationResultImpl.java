package com.netflix.astyanax.cql;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.ResultSet;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.OperationResult;

public class CqlOperationResultImpl<R> implements OperationResult<R> {

	private Host host;
	private R result; 
	
	public CqlOperationResultImpl(ResultSet rs, R result) {
		if (rs != null) {
			this.host = parseHostInfo(rs.getExecutionInfo().getQueriedHost());
		}
		this.result = result;
	}
	
	private Host parseHostInfo(com.datastax.driver.core.Host fromHost) {
		
		InetAddress add = fromHost.getAddress();
		
		Host toHost = new Host(add.getHostAddress(), -1);
		toHost.setRack(fromHost.getRack());
		return toHost;
	}
	
	@Override
	public Host getHost() {
		return host;
	}

	@Override
	public R getResult() {
		return result;
	}

	@Override
	public long getLatency() {
		return 0;
	}

	@Override
	public long getLatency(TimeUnit units) {
		return 0;
	}

	@Override
	public int getAttemptsCount() {
		return 0;
	}

	@Override
	public void setAttemptsCount(int count) {
	}

}
