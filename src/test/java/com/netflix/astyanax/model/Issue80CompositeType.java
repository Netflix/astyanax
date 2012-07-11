package com.netflix.astyanax.model;

import com.netflix.astyanax.annotations.Component;

public class Issue80CompositeType {
    @Component(ordinal=0)
    private String str1;
    
    @Component(ordinal=1)
    private String str2;
    
    @Component(ordinal=2) 
    private long timestamp;

    
    public Issue80CompositeType(String str1, String str2, long timestamp) {
        super();
        this.str1 = str1;
        this.str2 = str2;
        this.timestamp = timestamp;
    }

    public Issue80CompositeType() {
        
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((str1 == null) ? 0 : str1.hashCode());
        result = prime * result + ((str2 == null) ? 0 : str2.hashCode());
        result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Issue80CompositeType other = (Issue80CompositeType) obj;
        if (str1 == null) {
            if (other.str1 != null)
                return false;
        }
        else if (!str1.equals(other.str1))
            return false;
        if (str2 == null) {
            if (other.str2 != null)
                return false;
        }
        else if (!str2.equals(other.str2))
            return false;
        if (timestamp != other.timestamp)
            return false;
        return true;
    }

    public String getStr1() {
        return str1;
    }

    public String getStr2() {
        return str2;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "Issue80CompositeType [str1=" + str1 + ", str2=" + str2 + ", timestamp=" + timestamp + "]";
    }
    
    
}
