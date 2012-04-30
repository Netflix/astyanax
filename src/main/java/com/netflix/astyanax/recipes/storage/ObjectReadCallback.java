package com.netflix.astyanax.recipes.storage;

import java.nio.ByteBuffer;

public interface ObjectReadCallback {
    void onChunk(int chunk, ByteBuffer data);

    void onChunkException(int chunk, Exception exception);

    void onFailure(Exception exception);

    void onSuccess();
}
