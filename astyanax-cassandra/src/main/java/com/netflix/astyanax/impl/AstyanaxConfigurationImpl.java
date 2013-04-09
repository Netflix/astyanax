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

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.astyanax.AstyanaxConfiguration;
import com.netflix.astyanax.Clock;
import com.netflix.astyanax.clock.MicrosecondsSyncClock;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.partitioner.BigInteger127Partitioner;
import com.netflix.astyanax.partitioner.Murmur3Partitioner;
import com.netflix.astyanax.partitioner.Partitioner;
import com.netflix.astyanax.retry.RetryPolicy;
import com.netflix.astyanax.retry.RunOnce;

public class AstyanaxConfigurationImpl implements AstyanaxConfiguration {
    private ConsistencyLevel   defaultReadConsistencyLevel  = ConsistencyLevel.CL_ONE;
    private ConsistencyLevel   defaultWriteConsistencyLevel = ConsistencyLevel.CL_ONE;
    private Clock              clock                        = new MicrosecondsSyncClock();
    private RetryPolicy        retryPolicy                  = RunOnce.get();
    private ExecutorService    asyncExecutor                = Executors.newFixedThreadPool(5, 
            new ThreadFactoryBuilder().setDaemon(true)
                .setNameFormat("AstyanaxAsync-%d")
                .build());
    private NodeDiscoveryType   discoveryType               = NodeDiscoveryType.NONE;
    private int                 discoveryIntervalInSeconds  = 30;
    private ConnectionPoolType  connectionPoolType          = ConnectionPoolType.ROUND_ROBIN;
    private String              cqlVersion                  = null;
    private String              targetCassandraVersion      = "1.1";
    private Map<String, Partitioner> partitioners           = Maps.newHashMap();

    public AstyanaxConfigurationImpl() {
        partitioners.put(org.apache.cassandra.dht.RandomPartitioner.class.getCanonicalName(), BigInteger127Partitioner.get());
        try {
        	partitioners.put(org.apache.cassandra.dht.Murmur3Partitioner.class.getCanonicalName(), Murmur3Partitioner.get());
        }
        catch (NoClassDefFoundError exception) {
        	// We ignore this for backwards compatiblity with pre 1.2 cassandra.
        }
    }

    public AstyanaxConfigurationImpl setConnectionPoolType(ConnectionPoolType connectionPoolType) {
        this.connectionPoolType = connectionPoolType;
        return this;
    }

    @Override
    public ConnectionPoolType getConnectionPoolType() {
        return this.connectionPoolType;
    }

    @Override
    public ConsistencyLevel getDefaultReadConsistencyLevel() {
        return this.defaultReadConsistencyLevel;
    }

    public AstyanaxConfigurationImpl setDefaultReadConsistencyLevel(ConsistencyLevel cl) {
        this.defaultReadConsistencyLevel = cl;
        return this;
    }

    @Override
    public ConsistencyLevel getDefaultWriteConsistencyLevel() {
        return this.defaultWriteConsistencyLevel;
    }

    public AstyanaxConfigurationImpl setDefaultWriteConsistencyLevel(ConsistencyLevel cl) {
        this.defaultWriteConsistencyLevel = cl;
        return this;
    }

    @Override
    public Clock getClock() {
        return this.clock;
    }

    public AstyanaxConfigurationImpl setClock(Clock clock) {
        this.clock = clock;
        return this;
    }

    @Override
    public ExecutorService getAsyncExecutor() {
        return asyncExecutor;
    }

    public AstyanaxConfigurationImpl setAsyncExecutor(ExecutorService executor) {
        this.asyncExecutor.shutdown();
        this.asyncExecutor = executor;
        return this;
    }

    @Override
    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }

    public AstyanaxConfigurationImpl setRetryPolicy(RetryPolicy retryPolicy) {
        this.retryPolicy = retryPolicy;
        return this;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

    @Override
    public int getDiscoveryDelayInSeconds() {
        return discoveryIntervalInSeconds;
    }

    public AstyanaxConfigurationImpl setDiscoveryDelayInSeconds(int delay) {
        this.discoveryIntervalInSeconds = delay;
        return this;
    }

    @Override
    public NodeDiscoveryType getDiscoveryType() {
        return discoveryType;
    }

    public AstyanaxConfigurationImpl setDiscoveryType(NodeDiscoveryType discoveryType) {
        this.discoveryType = discoveryType;
        return this;
    }

    @Override
    public String getCqlVersion() {
        return cqlVersion;
    }

    public AstyanaxConfigurationImpl setCqlVersion(String cqlVersion) {
        this.cqlVersion = cqlVersion;
        return this;
    }

    @Override
    public String getTargetCassandraVersion() {
        return this.targetCassandraVersion;
    }
    
    public AstyanaxConfigurationImpl setTargetCassandraVersion(String version) {
        this.targetCassandraVersion = version;
        return this;
    }

    public AstyanaxConfigurationImpl registerPartitioner(String name, Partitioner partitioner) {
        this.partitioners.put(name, partitioner);
        return this;
    }
    
    public AstyanaxConfigurationImpl setPartitioners(Map<String, Partitioner> partitioners) {
        this.partitioners.putAll(partitioners);
        return this;
    }
    
    @Override
    public Partitioner getPartitioner(String partitionerName) throws Exception {
        Partitioner partitioner = partitioners.get(partitionerName);
        if (partitioner == null)
            throw new Exception("Unsupported partitioner " + partitionerName);
        return partitioner;
    }
}
