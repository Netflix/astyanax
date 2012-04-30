package com.netflix.astyanax.recipes.storage;

import java.util.concurrent.Callable;

public class ObjectInfoReader implements Callable<ObjectMetadata> {

    private final ChunkedStorageProvider provider;
    private final String objectName;

    public ObjectInfoReader(ChunkedStorageProvider provider, String objectName) {
        this.provider = provider;
        this.objectName = objectName;
    }

    @Override
    public ObjectMetadata call() throws Exception {
        return provider.readMetadata(objectName);
    }

}
