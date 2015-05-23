package com.netflix.astyanax.cql;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.QueryTrace;
import com.datastax.driver.core.ResultSet;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.OperationResult;

/**
 * Simple impl of {@link OperationResult} that tracks some basic info for every operation execution, such as
 * 1. The host that was used for the operation
 * 2. The operation attempt count
 * 3. The encapsulated result
 * 4. The overall latency for the operation. 
 * 
 * @author poberai
 *
 * @param <R>
 */
public class CqlOperationResultImpl<R> implements OperationResult<R> {

	private Host host;
	private R result; 
	private int attemptCount = 0;
	private long durationMicros = 0L;
	
	public CqlOperationResultImpl(ResultSet rs, R result) {
		this.host = parseHostInfo(rs);
		this.result = result;
		this.durationMicros = parseDuration(rs);
	}
	
	private Host parseHostInfo(ResultSet rs) {
		
		if (rs == null) {
			return null;
		}
		
		com.datastax.driver.core.Host fromHost = rs.getExecutionInfo().getQueriedHost();
		InetAddress add = fromHost.getAddress();
		
		Host toHost = new Host(add.getHostAddress(), -1);
		toHost.setRack(fromHost.getRack());
		return toHost;
	}
	
	private long parseDuration(ResultSet rs) {
		if (rs != null) {
			ExecutionInfo info = rs.getExecutionInfo();
			if (info !=null) {
				QueryTrace qt = info.getQueryTrace();
				if (qt != null) {
					return qt.getDurationMicros();
				}
			}
		}
		return 0L;
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
		return durationMicros;
	}

	@Override
	public long getLatency(TimeUnit units) {
		return units.convert(durationMicros, TimeUnit.MICROSECONDS);
	}

	@Override
	public int getAttemptsCount() {
		return attemptCount;
	}

	@Override
	public void setAttemptsCount(int count) {
		attemptCount = count;
	}

}
