package com.netflix.astyanax.recipes.storage;

public class NoOpObjectWriteCallback implements ObjectWriteCallback {

    @Override
    public void onChunk(int chunk, int size) {
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
