package com.netflix.astyanax.util;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.impl.AckingQueue;

public class BlockingAckingQueue implements AckingQueue {

    private LinkedBlockingQueue<MutationBatch> queue = Queues
            .newLinkedBlockingQueue();
    private ConcurrentMap<MutationBatch, Boolean> busy = Maps
            .newConcurrentMap();

    @Override
    public MutationBatch getNextMutation(long timeout, TimeUnit unit)
            throws InterruptedException {
        MutationBatch mutation = queue.poll(timeout, unit);
        if (mutation != null) {
            busy.put(mutation, true);
        }
        return mutation;
    }

    @Override
    public void pushMutation(MutationBatch m) throws Exception {
        queue.put(m);
    }

    @Override
    public void ackMutation(MutationBatch m) throws Exception {
        busy.remove(m);
    }

    @Override
    public void repushMutation(MutationBatch m) throws Exception {
        busy.remove(m);
        pushMutation(m);
    }

    @Override
    public int size() {
        return queue.size();
    }
}
