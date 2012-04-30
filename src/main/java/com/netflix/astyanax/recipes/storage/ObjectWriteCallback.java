package com.netflix.astyanax.recipes.storage;

public interface ObjectWriteCallback {
    void onChunk(int chunk, int size);

    void onChunkException(int chunk, Exception exception);

    void onFailure(Exception exception);

    void onSuccess();
}
