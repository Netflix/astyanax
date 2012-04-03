package com.netflix.astyanax.util;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.junit.Ignore;
import org.junit.Test;

import com.netflix.astyanax.Clock;
import com.netflix.astyanax.clock.MicrosecondsSyncClock;

public class TimeUUIDTest {
    @Test
    @Ignore
    public void testMicrosResolution() {
        Clock clock = new MicrosecondsSyncClock();
        long time = clock.getCurrentTime();

        UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMicros();
        long uuidTime = TimeUUIDUtils.getMicrosTimeFromUUID(uuid);

        Assert.assertEquals(time / 10000, uuidTime / 10000);
    }

    @Test
    public void testAddMicrosReslution() {
        UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMicros();
        long uuidTime = TimeUUIDUtils.getMicrosTimeFromUUID(uuid);

        UUID uuidPlusOneDay = TimeUUIDUtils.getMicrosTimeUUID(uuidTime
                + TimeUnit.DAYS.toMicros(1));
        long uuidTimePlusOneDay = TimeUUIDUtils
                .getMicrosTimeFromUUID(uuidPlusOneDay);

        Assert.assertEquals(TimeUnit.DAYS.toMicros(1), uuidTimePlusOneDay
                - uuidTime);
    }
}
