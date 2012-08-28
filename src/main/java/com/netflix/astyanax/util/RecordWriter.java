package com.netflix.astyanax.util;

import java.util.List;

import org.apache.cassandra.utils.Pair;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

/**
 *
 * @author elandau
 *
 */
public interface RecordWriter {
    void start() throws ConnectionException;

    void write(List<Pair<String, String>> record);

    void shutdown();

}
