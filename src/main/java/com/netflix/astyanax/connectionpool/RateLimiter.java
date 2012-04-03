package com.netflix.astyanax.connectionpool;

/**
 * Very very simple interface for a rate limiter. The basic idea is that clients
 * will call check() to determine if an operation may be performed. The concrete
 * rate limiter will update its internal state for each call to check
 * 
 * @author elandau
 */
public interface RateLimiter {
    boolean check();

    boolean check(long currentTimeMillis);
}
