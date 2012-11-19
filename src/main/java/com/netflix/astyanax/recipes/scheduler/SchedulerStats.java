package com.netflix.astyanax.recipes.scheduler;

public interface SchedulerStats {

    void incEmptyPartitionCount();

    void incLockContentionCount();

    void incProcessCount();

    void incReprocessCount();

    void incExpiredLockCount();

    void incSubmitTaskCount();
    
    void incFinishTaskCount();
}
