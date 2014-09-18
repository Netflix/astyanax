package com.netflix.astyanax.contrib.dualwrites;

public interface DualWritesUpdateListener {

    public void dualWritesEnabled();
    
    public void dualWritesDisabled();
    
    public void flipPrimaryAndSecondary(DualKeyspaceMetadata newDualKeyspaceSetup);
}
