package com.netflix.astyanax.connectionpool;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public interface LatencyScoreStrategy {
    public interface Listener {
        void onUpdate();

        void onReset();
    }

    /**
     * Single instance of this strategy associated with an endpoint
     */
    public interface Instance {
        /**
         * Add a single latency sample
         * 
         * @param sample
         * @param now
         */
        void addSample(long sample);

        /**
         * @return Get the cached score for this endpoint
         */
        double getScore();

        /**
         * Reset the score and any internal stats
         */
        void reset();

        /**
         * Update the score
         */
        void update();
    }

    /**
     * @return Return interval for updating the scores
     */
    int getUpdateInterval();
    
    /**
     * @return Return interval for clearing scores
     */
    int getResetInterval();
    
    /**
     * @return Return threshold after which hosts will be excluded if their score 
     * is getScoreThreshold() times larger than the best performing node
     */
    double getScoreThreshold();
    
    /**
     * @return Return threshold of blocked connections after which a host is excluded
     */
    int getBlockedThreshold();
    
    /**
     * @return  Get ratio for calculating minimum number of hosts that most remain in the pool
     */
    double getKeepRatio();

    /**
     * Update all instance scores
     */
    void update();

    /**
     * Reset all instance scores
     */
    void reset();

    /**
     * Create a new instance to associate with an endpoint
     * 
     * @return
     */
    Instance createInstance();

    /**
     * Remove the instance for an endpoint that is no longer being tracked
     * 
     * @param instance
     */
    void removeInstance(Instance instance);

    /**
     * Start updating stats for instances created using createInstance. This
     * usually spawns an update thread as well as a reset thread operating at
     * configurable intervals
     * 
     * @param listener
     */
    void start(Listener listener);

    /**
     * Shutdown the threads created by calling start()
     */
    void shutdown();

    /**
     * Sorts and filters a list of hosts by looking at their up state and score.
     * 
     * @param <CL>
     * @param pools
     * @param prioritized
     *            - Will be set to true if the filtered data is prioritized or
     *            not. If prioritized then the first element should be selected
     *            from by the load balancing strategy. Otherwise round robin
     *            could be used.
     * @return
     */
    <CL> List<HostConnectionPool<CL>> sortAndfilterPartition(List<HostConnectionPool<CL>> pools,
            AtomicBoolean prioritized);

}
