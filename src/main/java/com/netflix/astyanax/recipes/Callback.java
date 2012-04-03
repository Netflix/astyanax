package com.netflix.astyanax.recipes;

public interface Callback<T> {
    void handle(T object);
}
