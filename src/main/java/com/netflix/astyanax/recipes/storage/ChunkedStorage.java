package com.netflix.astyanax.recipes.storage;

import java.io.InputStream;
import java.io.OutputStream;

public class ChunkedStorage {
    public static ObjectWriter newWriter(ChunkedStorageProvider provider,
            String objectName, InputStream is) {
        return new ObjectWriter(provider, objectName, is);
    }

    public static ObjectReader newReader(ChunkedStorageProvider provider,
            String objectName, OutputStream os) {
        return new ObjectReader(provider, objectName, os);
    }

    public static ObjectDeleter newDeleter(ChunkedStorageProvider provider,
            String objectName) {
        return new ObjectDeleter(provider, objectName);
    }

    public static ObjectInfoReader newInfoReader(
            ChunkedStorageProvider provider, String objectName) {
        return new ObjectInfoReader(provider, objectName);
    }
}
