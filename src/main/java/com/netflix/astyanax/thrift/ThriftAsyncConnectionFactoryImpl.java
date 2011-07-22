package com.netflix.astyanax.thrift;

import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.TransportException;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TBinaryProtocol;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class ThriftAsyncConnectionFactoryImpl extends ThriftConnectionFactoryImpl<Cassandra.AsyncClient, ThriftAsyncConnectionFactoryImpl.ConnectionData> {
    private static final String NAME_FORMAT = "ThriftConnection<%s-%d>";

    static class ConnectionData
    {
        final TAsyncClientManager acm;
        final TNonblockingSocket clientSock;

        ConnectionData(TAsyncClientManager acm, TNonblockingSocket clientSock) {
            this.acm = acm;
            this.clientSock = clientSock;
        }
    }

    public ThriftAsyncConnectionFactoryImpl(ConnectionPoolConfiguration config) {
        super(config, NAME_FORMAT);
    }

    @Override
    protected void setKeyspace(Cassandra.AsyncClient client, String keyspaceName) throws TException, InvalidRequestException {
        final AtomicReference<Exception>    exceptionRef = new AtomicReference<Exception>();
        final CountDownLatch latch = new CountDownLatch(1);
        client.set_keyspace(keyspaceName, new AsyncMethodCallback<Cassandra.AsyncClient.set_keyspace_call>() {
            @Override
            public void onComplete(Cassandra.AsyncClient.set_keyspace_call response) {
                latch.countDown();
            }

            @Override
            public void onError(Exception exception) {
                exceptionRef.set(exception);
                latch.countDown();
            }
        });
        try {
            latch.await();

            Exception exception = exceptionRef.get();
            if ( exception != null ) {
                throw new TException(exception);
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TException("thread interrupted");
        }
    }

    @Override
    protected Cassandra.AsyncClient createClient(HostConnectionPool<Cassandra.AsyncClient> pool, AtomicReference<ThriftAsyncConnectionFactoryImpl.ConnectionData> connectionDataRef) throws ConnectionException {

        try {
            TAsyncClientManager     acm = new TAsyncClientManager();

            TNonblockingSocket      clientSock = new TNonblockingSocket(
                      pool.getHost().getIpAddress(), pool.getHost().getPort());
            connectionDataRef.set(new ConnectionData(acm, clientSock));
            
            return new Cassandra.AsyncClient(
                    new TBinaryProtocol.Factory(), acm, clientSock);
        }
        catch (IOException e) {
            throw new TransportException("Failed to open transport", e);	// TODO
        }
    }

    @Override
    protected void closeClient(Cassandra.AsyncClient client, ThriftAsyncConnectionFactoryImpl.ConnectionData connectionData) {
        try {
            connectionData.clientSock.flush();
        } catch (TTransportException e) {
            // ignore
        }
        finally {
            try {
                connectionData.clientSock.close();
            }
            finally {
                connectionData.acm.stop();
            }
        }
    }

    @Override
    protected boolean clientIsOpen(Cassandra.AsyncClient client, ThriftAsyncConnectionFactoryImpl.ConnectionData connectionData) {
        return connectionData.clientSock != null && connectionData.clientSock.isOpen();
    }
}
