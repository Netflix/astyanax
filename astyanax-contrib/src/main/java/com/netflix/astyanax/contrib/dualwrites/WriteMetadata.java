package com.netflix.astyanax.contrib.dualwrites;

/**
 * Simple class encapsulating metadata about a failed write. 
 * It represents the source and destination cluster / keyspace along with the CF and the row key.
 *  
 * @author poberai
 *
 */
public  class WriteMetadata {
    
    private final DualKeyspaceMetadata dualKeyspaceMetadata; 

    private final String cfName;
    private final String rowKey;
    private final Long uuid;
    
    public WriteMetadata(DualKeyspaceMetadata keyspaceMetadata, 
                               String cfName, String rowKey) {
        
        this.dualKeyspaceMetadata = keyspaceMetadata;
        this.rowKey = rowKey;
        this.cfName = cfName;
        this.uuid = System.currentTimeMillis();
    }

    public String getPrimaryCluster() {
        return dualKeyspaceMetadata.getPrimaryCluster();
    }

    public String getSecondaryCluster() {
        return dualKeyspaceMetadata.getSecondaryCluster();
    }

    public String getPrimaryKeyspace() {
        return dualKeyspaceMetadata.getPrimaryKeyspaceName();
    }

    public String getSecondaryKeyspace() {
        return dualKeyspaceMetadata.getSecondaryKeyspaceName();
    }

    public String getCFName() {
        return cfName;
    }

    public String getRowKey() {
        return rowKey;
    }

    public Long getUuid() {
        return uuid;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + dualKeyspaceMetadata.hashCode();
        result = prime * result + ((cfName == null) ? 0 : cfName.hashCode());
        result = prime * result + ((rowKey == null) ? 0 : rowKey.hashCode());
        result = prime * result + ((uuid == null) ? 0 : uuid.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        
        WriteMetadata other = (WriteMetadata) obj;
        boolean equals = true; 
        equals &= dualKeyspaceMetadata != null ? dualKeyspaceMetadata.equals(other.dualKeyspaceMetadata) : other.dualKeyspaceMetadata == null;
        equals &= cfName               != null ? cfName.equals(other.cfName) : other.cfName == null;
        equals &= rowKey               != null ? rowKey.equals(other.rowKey) : other.rowKey == null;
        equals &= uuid                 != null ? uuid.equals(other.uuid) : other.uuid == null;
        
        return equals;
    }

    @Override
    public String toString() {
        return "FailedWriteMetadata [" + dualKeyspaceMetadata +  
                ", cfName=" + cfName + ", rowKey=" + rowKey + ", uuid=" + uuid + "]";
    } 
    
}