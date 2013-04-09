package com.netflix.astyanax;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.netflix.astyanax.connectionpool.ConnectionFactory;
import com.netflix.astyanax.connectionpool.ConnectionPool;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.NodeDiscovery;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.impl.NodeDiscoveryImpl;
import com.netflix.astyanax.connectionpool.impl.BagOfConnectionsConnectionPoolImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.RoundRobinConnectionPoolImpl;
import com.netflix.astyanax.connectionpool.impl.TokenAwareConnectionPoolImpl;
import com.netflix.astyanax.impl.FilteringHostSupplier;
import com.netflix.astyanax.impl.RingDescribeHostSupplier;
import com.netflix.astyanax.shallows.EmptyKeyspaceTracerFactory;

/**
 * This object tracks the context of an astyanax instance of either a Cluster or
 * Keyspace
 * 
 * @author elandau
 * 
 * @param <T>
 */
public class AstyanaxContext<T> {
    private final ConnectionPool<?> cp;
    private final NodeDiscovery discovery;
    private final ConnectionPoolConfiguration cpConfig;
    private final AstyanaxConfiguration asConfig;
    private final String clusterName;
    private final String keyspaceName;
    private final T client;
    private final ConnectionPoolMonitor monitor;

    public static class Builder {
        protected ConnectionPool<?> cp;
        protected NodeDiscovery discovery;
        protected ConnectionPoolConfiguration cpConfig;
        protected AstyanaxConfiguration asConfig;
        protected String clusterName;
        protected String keyspaceName;
        protected KeyspaceTracerFactory tracerFactory = EmptyKeyspaceTracerFactory.getInstance();
        protected Supplier<List<Host>> hostSupplier;
        protected ConnectionPoolMonitor monitor = new CountingConnectionPoolMonitor();

        public Builder forCluster(String clusterName) {
            this.clusterName = clusterName;
            return this;
        }

        public Builder forKeyspace(String keyspaceName) {
            this.keyspaceName = keyspaceName;
            return this;
        }

        public Builder withConnectionPoolConfiguration(ConnectionPoolConfiguration cpConfig) {
            this.cpConfig = cpConfig;
            return this;
        }

        public Builder withAstyanaxConfiguration(AstyanaxConfiguration asConfig) {
            this.asConfig = asConfig;
            return this;
        }

        public Builder withHostSupplier(Supplier<List<Host>> supplier) {
            this.hostSupplier = supplier;
            return this;
        }

        public Builder withTracerFactory(KeyspaceTracerFactory tracerFactory) {
            this.tracerFactory = tracerFactory;
            return this;
        }

        public Builder withConnectionPoolMonitor(ConnectionPoolMonitor monitor) {
            this.monitor = monitor;
            return this;
        }

        public NodeDiscoveryType getNodeDiscoveryType() {
            if (cpConfig.getSeeds() != null) {
                if (asConfig.getConnectionPoolType() == ConnectionPoolType.TOKEN_AWARE)
                    return NodeDiscoveryType.RING_DESCRIBE;
            }
            else {
                if (asConfig.getConnectionPoolType() == ConnectionPoolType.TOKEN_AWARE) {
                    return NodeDiscoveryType.TOKEN_AWARE;
                }
                else {
                    return NodeDiscoveryType.DISCOVERY_SERVICE;
                }
            }
            return asConfig.getDiscoveryType();
        }

        protected <T> ConnectionPool<T> createConnectionPool(ConnectionFactory<T> connectionFactory) {
            ConnectionPool<T> connectionPool = null;
            switch (asConfig.getConnectionPoolType()) {
            case TOKEN_AWARE:
                connectionPool = new TokenAwareConnectionPoolImpl<T>(cpConfig, connectionFactory, monitor);
                break;

            case BAG:
                connectionPool = new BagOfConnectionsConnectionPoolImpl<T>(cpConfig, connectionFactory, monitor);
                break;

            case ROUND_ROBIN:
            default:
                connectionPool = new RoundRobinConnectionPoolImpl<T>(cpConfig, connectionFactory, monitor);
                break;
            }

            if (hostSupplier != null) {
                connectionPool.setHosts(hostSupplier.get());
            }

            return connectionPool;
        }

        public <T> AstyanaxContext<Keyspace> buildKeyspace(AstyanaxTypeFactory<T> factory) {
            this.cpConfig.initialize();
            
            ConnectionPool<T> cp = createConnectionPool(factory.createConnectionFactory(asConfig, cpConfig, tracerFactory,
                    monitor));
            this.cp = cp;

            final Keyspace keyspace = factory.createKeyspace(keyspaceName, cp, asConfig, tracerFactory);

            Supplier<List<Host>> supplier = null;

            switch (getNodeDiscoveryType()) {
            case DISCOVERY_SERVICE:
                Preconditions.checkNotNull(hostSupplier, "Missing host name supplier");
                supplier = hostSupplier;
                break;

            case RING_DESCRIBE:
                supplier = new RingDescribeHostSupplier(keyspace, cpConfig.getPort(), cpConfig.getLocalDatacenter());
                break;

            case TOKEN_AWARE:
                if (hostSupplier == null) {
                    supplier = new RingDescribeHostSupplier(keyspace, cpConfig.getPort(), cpConfig.getLocalDatacenter());
                }
                else {
                    supplier = new FilteringHostSupplier(new RingDescribeHostSupplier(keyspace, cpConfig.getPort(), cpConfig.getLocalDatacenter()),
                            hostSupplier);
                }
                break;

            case NONE:
                supplier = null;
                break;
            }

            if (supplier != null) {
                discovery = new NodeDiscoveryImpl(StringUtils.join(Arrays.asList(clusterName, keyspaceName), "_"),
                        asConfig.getDiscoveryDelayInSeconds() * 1000, supplier, cp);
            }

            return new AstyanaxContext<Keyspace>(this, keyspace);
        }

        public <T> AstyanaxContext<Cluster> buildCluster(AstyanaxTypeFactory<T> factory) {
            this.cpConfig.initialize();
            
            ConnectionPool<T> cp = createConnectionPool(factory.createConnectionFactory(asConfig, cpConfig, tracerFactory,
                    monitor));
            this.cp = cp;

            if (hostSupplier != null) {
                discovery = new NodeDiscoveryImpl(clusterName, asConfig.getDiscoveryDelayInSeconds() * 1000,
                        hostSupplier, cp);
            }
            return new AstyanaxContext<Cluster>(this, factory.createCluster(cp, asConfig, tracerFactory));
        }
    }

    private AstyanaxContext(Builder builder, T client) {
        this.cpConfig = builder.cpConfig;
        this.asConfig = builder.asConfig;
        this.cp = builder.cp;
        this.clusterName = builder.clusterName;
        this.keyspaceName = builder.keyspaceName;
        this.client = client;
        this.discovery = builder.discovery;
        this.monitor = builder.monitor;
    }

    /**
     * @deprecated  This should be called getClient
     * @return
     */
    public T getEntity() {
        return getClient();
    }

    public T getClient() {
        return this.client;
    }

    public ConnectionPool<?> getConnectionPool() {
        return this.cp;
    }

    public ConnectionPoolConfiguration getConnectionPoolConfiguration() {
        return cpConfig;
    }

    public AstyanaxConfiguration getAstyanaxConfiguration() {
        return asConfig;
    }

    public NodeDiscovery getNodeDiscovery() {
        return this.discovery;
    }

    public ConnectionPoolMonitor getConnectionPoolMonitor() {
        return this.monitor;
    }

    public void start() {
        cp.start();
        if (discovery != null)
            discovery.start();
    }

    public void shutdown() {
        if (discovery != null)
            discovery.shutdown();
        cp.shutdown();
    }

    public String getClusterName() {
        return this.clusterName;
    }

    public String getKeyspaceName() {
        return this.keyspaceName;
    }
}
