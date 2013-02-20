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
package com.netflix.astyanax.connectionpool.exceptions;

import java.util.concurrent.TimeUnit;

import com.netflix.astyanax.connectionpool.Host;

/**
 * Connection exception caused by an error in the connection pool or a transport
 * error related to the connection itself. Application errors are derived from
 * OperationException.
 * 
 * @author elandau
 * 
 */
public abstract class ConnectionException extends Exception {
    /**
     * 
     */
    private static final long serialVersionUID = -3476496346094715988L;
    private Host host = Host.NO_HOST;
    private long latency = 0;
    private long latencyWithPool = 0;
    private int attemptCount = 0;

    public ConnectionException(String message) {
        super(message);
    }

    public ConnectionException(Throwable t) {
        super(t);
    }

    public ConnectionException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConnectionException setHost(Host host) {
        this.host = host;
        return this;
    }

    public Host getHost() {
        return this.host;
    }

    public ConnectionException setLatency(long latency) {
        this.latency = latency;
        return this;
    }

    public long getLatency() {
        return this.latency;
    }

    public long getLatency(TimeUnit units) {
        return units.convert(this.latency, TimeUnit.NANOSECONDS);
    }

    public ConnectionException setLatencyWithPool(long latency) {
        this.latencyWithPool = latency;
        return this;
    }

    public long getLatencyWithPool() {
        return this.latencyWithPool;
    }

    @Override
    public String getMessage() {
        return new StringBuilder()
            .append(getClass().getSimpleName())
            .append(": [")
            .append(  "host="    ).append(host.toString())
            .append(", latency=" ).append(latency).append("(").append(latencyWithPool).append(")")
            .append(", attempts=").append(attemptCount)
            .append("]")
            .append(super.getMessage())
            .toString();
    }

    public String getOriginalMessage() {
        return super.getMessage();
    }

    public ConnectionException setAttempt(int attemptCount) {
        this.attemptCount = attemptCount;
        return this;
    }
}
