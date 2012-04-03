package com.netflix.astyanax.shallows;

import com.netflix.astyanax.connectionpool.BadHostDetector;

public class EmptyBadHostDetectorImpl implements BadHostDetector {

    private static EmptyBadHostDetectorImpl instance = new EmptyBadHostDetectorImpl();

    public static EmptyBadHostDetectorImpl getInstance() {
        return instance;
    }

    private EmptyBadHostDetectorImpl() {

    }

    @Override
    public Instance createInstance() {
        return new Instance() {
            @Override
            public boolean addTimeoutSample() {
                return false;
            }
        };
    }

    @Override
    public void removeInstance(Instance instance) {
    }
}
