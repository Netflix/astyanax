package com.netflix.astyanax.connectionpool.impl;

import org.junit.Assert;
import org.junit.Test;

import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.exceptions.NoAvailableHostsException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;

public class Slf4jConnectionPoolMonitorImplTest {

    @Test
    public void testNotFoundCounter() {
        Slf4jConnectionPoolMonitorImpl monitor = new Slf4jConnectionPoolMonitorImpl();
        monitor.incOperationFailure(Host.NO_HOST, new NoAvailableHostsException("regular exception"));
        Assert.assertEquals(0, monitor.notFoundCount());
        Assert.assertEquals(1, monitor.getOperationFailureCount());
        monitor.incOperationFailure(Host.NO_HOST, new NotFoundException("not found exception"));
        Assert.assertEquals(1, monitor.notFoundCount());
        Assert.assertEquals(1, monitor.getOperationFailureCount());
    }
}
