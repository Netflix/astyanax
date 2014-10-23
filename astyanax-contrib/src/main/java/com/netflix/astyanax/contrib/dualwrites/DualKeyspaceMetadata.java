package com.netflix.astyanax.contrib.dualwrites;

/**
 * Simple class representing the keyspace pair i.e   Source Keyspace --> Destination Keyspace
 * 
 * @author poberai
 *
 */
public class DualKeyspaceMetadata {

    private final String primaryCluster;
    private final String primaryKeyspaceName;
    private final String secondaryCluster;
    private final String secondaryKeyspaceName;
    
    public DualKeyspaceMetadata(String primaryCluster, String primaryKeyspaceName, String secondaryCluster, String secondaryKeyspaceName) {
        if (primaryCluster == null || primaryKeyspaceName == null) {
            throw new RuntimeException("primaryCluster and primaryKeyspaceName cannot be NULL");
        }
        if (secondaryCluster == null || secondaryKeyspaceName == null) {
            throw new RuntimeException("secondaryCluster and secondaryKeyspaceName cannot be NULL");
        }
        this.primaryCluster = primaryCluster;
        this.primaryKeyspaceName = primaryKeyspaceName;
        this.secondaryCluster = secondaryCluster;
        this.secondaryKeyspaceName = secondaryKeyspaceName;
    }

    public String getPrimaryCluster() {
        return primaryCluster;
    }

    public String getPrimaryKeyspaceName() {
        return primaryKeyspaceName;
    }

    public String getSecondaryCluster() {
        return secondaryCluster;
    }

    public String getSecondaryKeyspaceName() {
        return secondaryKeyspaceName;
    }

    public boolean isReverse(DualKeyspaceMetadata newDualKeyspaceSetup) {
        
        return (!this.equals(newDualKeyspaceSetup) && 
                this.primaryCluster.equals(newDualKeyspaceSetup.getSecondaryCluster()) &&
                this.secondaryCluster.equals(newDualKeyspaceSetup.getPrimaryCluster()));
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((primaryCluster == null) ? 0 : primaryCluster.hashCode());
        result = prime * result + ((primaryKeyspaceName == null) ? 0 : primaryKeyspaceName.hashCode());
        result = prime * result + ((secondaryCluster == null) ? 0 : secondaryCluster.hashCode());
        result = prime * result + ((secondaryKeyspaceName == null) ? 0 : secondaryKeyspaceName.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        
        DualKeyspaceMetadata other = (DualKeyspaceMetadata) obj;
        boolean equals = true; 
        equals &= primaryCluster        != null ? primaryCluster.equals(other.primaryCluster)               : other.primaryCluster == null;
        equals &= primaryKeyspaceName   != null ? primaryKeyspaceName.equals(other.primaryKeyspaceName)     : other.primaryKeyspaceName == null;
        equals &= secondaryCluster      != null ? secondaryCluster.equals(other.secondaryCluster)           : other.secondaryCluster == null;
        equals &= secondaryKeyspaceName != null ? secondaryKeyspaceName.equals(other.secondaryKeyspaceName) : other.secondaryKeyspaceName == null;
        
        return equals;
    }

    @Override
    public String toString() {
        return "DualKeyspaceMetadata [primaryCluster=" + primaryCluster + ", primaryKeyspaceName=" + primaryKeyspaceName 
                + ", secondaryCluster=" + secondaryCluster + ", secondaryKeyspaceName=" + secondaryKeyspaceName + "]";
    }
}
