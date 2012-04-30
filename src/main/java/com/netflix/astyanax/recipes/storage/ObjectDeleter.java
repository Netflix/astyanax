package com.netflix.astyanax.recipes.storage;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObjectDeleter implements Callable<Void> {
    private static final Logger LOG = LoggerFactory
            .getLogger(ObjectDeleter.class);

    private final ChunkedStorageProvider provider;
    private final String objectName;
    private Integer chunkCount = null; // This will default to all being deleted

    public ObjectDeleter(ChunkedStorageProvider provider, String objectName) {
        this.provider = provider;
        this.objectName = objectName;
    }

    public ObjectDeleter withChunkCountToDelete(int count) {
        this.chunkCount = count;
        return this;
    }

    @Override
    public Void call() throws Exception {
        LOG.info("Deleting " + objectName);
        provider.deleteObject(objectName, chunkCount);
        return null;
    }
}
