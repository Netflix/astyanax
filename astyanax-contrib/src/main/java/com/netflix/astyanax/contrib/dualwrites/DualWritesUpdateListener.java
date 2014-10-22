package com.netflix.astyanax.contrib.dualwrites;

/**
 * Notification based listener that gets update from some controller when changing the behavior of dual writes. 
 * This is what folks will need to implement to be able to react to the dual writes migration process. 
 * @see {@link DualWritesKeyspace}
 * 
 * @author poberai
 *
 */
public interface DualWritesUpdateListener {

    /**
     * Start dual writes
     */
    public void dualWritesEnabled();
    
    /**
     * Stop dual writes
     */
    public void dualWritesDisabled();
    
    /**
     * Flip roles of primary and secondary keyspaces. 
     * 
     * @param newDualKeyspaceSetup
     */
    public void flipPrimaryAndSecondary();
}
