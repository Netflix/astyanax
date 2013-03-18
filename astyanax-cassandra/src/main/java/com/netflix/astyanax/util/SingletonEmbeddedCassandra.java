package com.netflix.astyanax.util;

import com.netflix.astyanax.test.EmbeddedCassandra;

public class SingletonEmbeddedCassandra {
    
    private static class Holder {
        private static final SingletonEmbeddedCassandra instance = new SingletonEmbeddedCassandra();
    }

    private final EmbeddedCassandra         cassandra;

    private SingletonEmbeddedCassandra() {
        try {
            cassandra = new EmbeddedCassandra();
            cassandra.start();
        } catch (Exception e) {
            throw new RuntimeException("Failed to start embedded cassandra", e);
        }
    }
    
    public static SingletonEmbeddedCassandra getInstance() {
        return Holder.instance;
    }
    
    public void shutdown() {
        try {
            cassandra.stop();
        }
        catch (Exception e) {
            
        }
    }
}
