package com.netflix.astyanax.test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.thrift.CassandraDaemon;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class EmbeddedCassandra {
    private static final Logger LOG = LoggerFactory.getLogger(EmbeddedCassandra.class);

    public static final int     DEFAULT_PORT = 9160;
    public static final int     DEFAULT_STORAGE_PORT = 7000;

    private final ExecutorService service = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("EmbeddedCassandra-%d")
                .build());

    private final CassandraDaemon cassandra;
    private final File dataDir;

    public EmbeddedCassandra() throws IOException {
        this(createTempDir(), "TestCluster", DEFAULT_PORT, DEFAULT_STORAGE_PORT);
    }

    private static File createTempDir() {
        File tempDir = Files.createTempDir();
        tempDir.deleteOnExit();
        return tempDir;
    }

    public EmbeddedCassandra(File dataDir, String clusterName, int port, int storagePort) throws IOException {
        LOG.info("Starting cassandra in dir " + dataDir);
        this.dataDir = dataDir;
        dataDir.mkdirs();

        URL         templateUrl = ClassLoader.getSystemClassLoader().getResource("cassandra-template.yaml");
        Preconditions.checkNotNull(templateUrl, "Cassandra config template is null");
        String      baseFile = Resources.toString(templateUrl, Charset.defaultCharset());

        String      newFile = baseFile.replace("$DIR$", dataDir.getPath());
        newFile = newFile.replace("$PORT$", Integer.toString(port));
        newFile = newFile.replace("$STORAGE_PORT$", Integer.toString(storagePort));
        newFile = newFile.replace("$CLUSTER$", clusterName);

        File        configFile = new File(dataDir, "cassandra.yaml");
        Files.write(newFile, configFile, Charset.defaultCharset());

        LOG.info("Cassandra config file: " + configFile.getPath());
        System.setProperty("cassandra.config", "file://" + configFile.getPath());

        try {
            cassandra = new CassandraDaemon();
            cassandra.init(null);
        }
        catch (IOException e) {
            LOG.error("Error initializing embedded cassandra", e);
            throw e;
        }
        LOG.info("Started cassandra deamon");
    }

    public void start() throws IOException, TTransportException {
        service.submit(new Callable<Object>(){
                @Override
                public Object call() throws Exception
                {
                    try {
                        cassandra.start();
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                    return null;
                }
            }
        );
    }

    public void stop() {
        service.shutdownNow();
        cassandra.deactivate();
    }
}
