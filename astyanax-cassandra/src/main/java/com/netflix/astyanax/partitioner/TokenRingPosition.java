package com.netflix.astyanax.partitioner;

import org.apache.cassandra.dht.Token;

public class TokenRingPosition implements RingPosition {
    private final Token _token;

    public TokenRingPosition(Token token) {
        _token = token;
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compareTo(RingPosition o) {
        return _token.compareTo(((TokenRingPosition) o)._token);
    }

    @Override
    public boolean equals(Object o) {
        return this == o ||
                getClass() == o.getClass() && _token.equals(((TokenRingPosition) o)._token);
    }

    @Override
    public int hashCode() {
        return _token.hashCode();
    }

    @Override
    public String toString() {
        return _token.toString();
    }
}
