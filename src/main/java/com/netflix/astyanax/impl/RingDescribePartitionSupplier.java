package com.netflix.astyanax.impl;

import org.apache.cassandra.dht.IPartitioner;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

/**
 * Queries the Cassandra cluster to determine the configured {@code IPartitioner}, eg. {@code RandomPartitioner} or
 * {@code ByteOrderedPartitioner}.
 * <p>
 * Note that some methods on {@code IPartitioner} like {@code describeOwnership()} can't be called from a client
 * library.
 */
public class RingDescribePartitionSupplier implements Supplier<IPartitioner> {
    private final Cluster cluster;

    public static Supplier<IPartitioner> create(Cluster cluster) {
        // A cluster's partitioner can't change without a rebuild from scratch.  Cache the partitioner setting forever.
        return Suppliers.memoize(new RingDescribePartitionSupplier(cluster));
    }

    private RingDescribePartitionSupplier(Cluster cluster) {
        this.cluster = cluster;
    }

    @Override
    public IPartitioner get() {
        String partitioner;
        try {
            partitioner = this.cluster.describePartitioner();
        } catch (ConnectionException e) {
            throw new RuntimeException(e);
        }
        try {
            return (IPartitioner) Class.forName(partitioner).newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Unable to instantiate partitioner: " + partitioner, e);
        }
    }
}
