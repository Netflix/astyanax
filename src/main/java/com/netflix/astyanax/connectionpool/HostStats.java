package com.netflix.astyanax.connectionpool;

import java.util.Date;

public interface HostStats {

    boolean isUp();

    boolean isInRing();

    Date getTimeCreated();

    HostConnectionPool<?> getPool();

    long getUpTime();

    long getSuccessCount();

    long getErrorCount();

    long getConnectionsClosed();

    long getConnectionsCreated();

    long getConnectionsCreateFailed();

    long getTimesUp();

    long getTimesDown();

    void setPool(HostConnectionPool<?> pool);

}
