package com.netflix.astyanax.recipes.queue;

public interface MessageQueueStats {

    void incEmptyPartitionCount();

    void incLockContentionCount();

    void incProcessCount();

    void incReprocessCount();

    void incExpiredLockCount();

    void incSendMessageCount();
    
    void incFinishMessageCount();

    void incInvalidMessageCount();
}
