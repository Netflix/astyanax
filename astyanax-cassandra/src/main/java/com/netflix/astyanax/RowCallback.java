package com.netflix.astyanax;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Rows;

public interface RowCallback<K, C> {
    /**
     * Notification for each block of rows.
     * 
     * @param rows
     */
    void success(Rows<K, C> rows);

    /**
     * Notification of an error calling cassandra. In your handler you can
     * implement your own backoff logic and return true to retry or false to
     * stop the query.
     * 
     * @param e
     * @return true to retry or false to exit
     */
    boolean failure(ConnectionException e);
}
