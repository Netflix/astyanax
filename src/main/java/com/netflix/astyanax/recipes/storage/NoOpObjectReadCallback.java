package com.netflix.astyanax.recipes.storage;

import java.nio.ByteBuffer;

public class NoOpObjectReadCallback implements ObjectReadCallback {
    @Override
    public void onChunk(int chunk, ByteBuffer data) {
    }

    @Override
    public void onChunkException(int chunk, Exception exception) {
    }

    @Override
    public void onFailure(Exception exception) {
    }

    @Override
    public void onSuccess() {
    }
}
