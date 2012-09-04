package com.netflix.astyanax.util;

import java.io.IOException;
import java.util.List;

import org.apache.cassandra.utils.Pair;

/**
 *
 * @author elandau
 *
 */
public interface RecordReader {
    List<Pair<String, String>> next() throws IOException;

    void shutdown();

    void start() throws IOException;
}
