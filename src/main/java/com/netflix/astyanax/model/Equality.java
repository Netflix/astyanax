package com.netflix.astyanax.model;

public enum Equality {
    LESS_THAN((byte) -1), 
    GREATER_THAN_EQUALS((byte)-1),
    EQUAL((byte) 0), 
    GREATER_THAN((byte) 1),
    LESS_THAN_EQUALS((byte)1);

    private final byte equality;

    Equality(byte equality) {
    	this.equality = equality;
    }

    public byte toByte() {
      return equality;
    }

    public static Equality fromByte(byte equality) {
    	if (equality > 0) {
    		return GREATER_THAN;
    	}
    	if (equality < 0) {
    		return LESS_THAN;
    	}
    	return EQUAL;
    }
}	
