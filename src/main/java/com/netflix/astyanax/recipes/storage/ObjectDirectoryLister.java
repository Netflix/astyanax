package com.netflix.astyanax.recipes.storage;

import java.util.Map;
import java.util.concurrent.Callable;

public class ObjectDirectoryLister implements Callable<Map<String, ObjectMetadata>> {

    private final ChunkedStorageProvider provider;
    private final String path;

    public ObjectDirectoryLister(ChunkedStorageProvider provider, String path) {
        this.provider = provider;
        this.path = path;
    }

    @Override
    public Map<String, ObjectMetadata> call() throws Exception {
        return null;
    }

}
