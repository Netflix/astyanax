/**
 *  ConsistencyLevel.java
 *  dsmirnov Mar 31, 2011
 */
package com.netflix.astyanax.model;

/**
 * Consistency Level thin abstraction
 * 
 * @author dsmirnov
 * 
 */
public enum ConsistencyLevel {
    CL_ONE, CL_QUORUM, CL_ALL, CL_ANY, CL_EACH_QUORUM, CL_LOCAL_QUORUM, CL_TWO, CL_THREE;
}
