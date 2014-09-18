package com.netflix.astyanax.contrib.dualwrites;

import java.util.Collection;

import com.netflix.astyanax.Execution;

public interface DualWritesStrategy {

    public Execution<Void> wrapExecutions(Execution<Void> primary, Execution<Void> secondary, Collection<WriteMetadata> writeMetadata); 

}
