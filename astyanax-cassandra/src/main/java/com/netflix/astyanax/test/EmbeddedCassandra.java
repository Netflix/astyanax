/*******************************************************************************
 * Copyright 2011 Netflix
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.netflix.astyanax.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.cassandra.service.CassandraDaemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Use EmbeddedCassandraFactory
 * @author elandau
 *
 */
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

    public EmbeddedCassandra(String dataDir) throws IOException {
        this(new File(dataDir), "TestCluster", DEFAULT_PORT, DEFAULT_STORAGE_PORT);
    }

    public EmbeddedCassandra(File dataDir) throws IOException {
        this(dataDir, "TestCluster", DEFAULT_PORT, DEFAULT_STORAGE_PORT);
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

        InputStream is = null;

        try {
            URL         templateUrl = EmbeddedCassandra.class.getClassLoader().getResource("cassandra-template.yaml");
            Preconditions.checkNotNull(templateUrl, "Cassandra config template is null");
            String      baseFile = Resources.toString(templateUrl, Charset.defaultCharset());

            String      newFile = baseFile.replace("$DIR$", dataDir.getPath());
            newFile = newFile.replace("$PORT$", Integer.toString(port));
            newFile = newFile.replace("$STORAGE_PORT$", Integer.toString(storagePort));
            newFile = newFile.replace("$CLUSTER$", clusterName);

            File        configFile = new File(dataDir, "cassandra.yaml");
            Files.write(newFile, configFile, Charset.defaultCharset());

            LOG.info("Cassandra config file: " + configFile.getPath());
            System.setProperty("cassandra.config", "file:" + configFile.getPath());

            try {
                cassandra = new CassandraDaemon();
                cassandra.init(null);
            }
            catch (IOException e) {
                LOG.error("Error initializing embedded cassandra", e);
                throw e;
            }
        }
        finally {
            Closeables.closeQuietly(is);
        }
        LOG.info("Started cassandra deamon");
    }

    public void start()  {
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
